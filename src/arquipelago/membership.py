"""Entrada no cluster, heartbeats e detecção de falhas (modelo crash-stop).

Líder
    A cada HEARTBEAT_INTERVAL envia Heartbeat a todos os membros (em paralelo).
    O heartbeat leva a lista de membros e a tabela de rotas das ilhas.
    Um membro sem resposta há MEMBER_FAILURE_TIMEOUT é declarado morto e
    removido; suas ilhas são reatribuídas pelo JobManager.

Seguidor
    Se não recebe heartbeat do líder há LEADER_FAILURE_TIMEOUT, considera o
    líder morto e inicia uma eleição (Bully). Sem líder conhecido, tenta entrar
    no cluster (Join) pelos endereços conhecidos; se ninguém for líder após
    JOIN_GRACE, convoca eleição.

Bully "clássico": um nó que volta (ou entra) com id maior que o do líder
primeiro recebe o estado replicado e depois convoca eleição, assumindo a
liderança sem perder o estado.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor

from . import config
from .rpc import arquipelago_pb2 as pb

log = logging.getLogger("arquipelago")


class Membership:
    def __init__(self, node):
        self.node = node
        self.pool = ThreadPoolExecutor(max_workers=16, thread_name_prefix="hb")
        self.last_heartbeat = 0.0          # seguidor: último heartbeat válido do líder
        self.leader_store_version = 0
        self.leader_store_term = 0
        self.leaderless_since = time.monotonic()
        self.last_ok: dict[int, float] = {}  # líder: último heartbeat respondido por membro
        self.running: dict[int, tuple[str, list[int], float]] = {}  # ilhas em execução por membro
        self._hb_inflight: set[int] = set()
        self._join_inflight = False
        self.joined_at = 0.0

    # ------------------------------------------------------------ laço principal

    def run(self) -> None:
        while self.node.running:
            try:
                if self.node.is_leader:
                    self._leader_tick()
                else:
                    self._follower_tick()
            except Exception:
                log.exception("erro no laço de pertencimento")
            time.sleep(config.HEARTBEAT_INTERVAL)

    def leader_seen(self) -> None:
        self.last_heartbeat = time.monotonic()

    def on_become_leader(self) -> None:
        now = time.monotonic()
        self.last_ok = {m.id: now for m in self.node.member_list(include_self=False)}
        self.running.clear()

    # ------------------------------------------------------------ lado do seguidor

    def _follower_tick(self) -> None:
        node = self.node
        now = time.monotonic()
        if node.leader_id is None:
            if not self._join_inflight:
                self._join_inflight = True
                self.pool.submit(self._try_join)
            if now - self.leaderless_since > config.JOIN_GRACE:
                node.election.start("nenhum líder encontrado")
            return
        if now - self.last_heartbeat > config.LEADER_FAILURE_TIMEOUT:
            dead = node.forget_leader()
            lam = node.lamport.tick()
            node.event("leader_suspected", lamport=lam, leader=dead)
            node.local_log(f"Líder {dead} não responde há {config.LEADER_FAILURE_TIMEOUT:.0f}s -> considerado falho")
            node.election.start(f"líder {dead} falhou")
            return
        caught_up = (node.store.term == self.leader_store_term and node.store.version > 0
                     and node.store.version >= self.leader_store_version)
        settled = now - self.joined_at > 2.0  # dá tempo de o relógio ser alinhado antes
        if node.preempt and node.id > (node.leader_id or 0) and caught_up and settled:
            node.election.start(f"id {node.id} > líder {node.leader_id} (Bully: o maior assume)")

    def _try_join(self) -> None:
        node = self.node
        try:
            with node.lock:
                targets = set(node.seeds) | {m.address for m in node.known_nodes()}
            for addr in sorted(targets):
                if node.leader_id is not None or not node.running:
                    return
                if self._join(addr):
                    return
        finally:
            self._join_inflight = False

    def _join(self, addr: str, follow_hint: bool = True) -> bool:
        node = self.node
        req = pb.JoinRequest(h=node.hdr(), node=node.info(), seen_address=addr)
        rep = node.call(addr, "Join", req, timeout=1.0)
        if rep is None:
            return False
        from .node import Member
        responder = Member.from_pb(rep.responder)
        responder.address = addr  # o endereço que funcionou
        node.add_known(responder)
        if rep.accepted:
            node.set_members([Member.from_pb(m) for m in rep.members])
            self.joined_at = time.monotonic()
            node.accept_leader(responder, rep.h.term)
            node.local_log(f"Entrei no cluster do líder {responder.id} ({addr})")
            return True
        if follow_hint and rep.leader_id and rep.leader_address and rep.leader_address != addr:
            return self._join(rep.leader_address, follow_hint=False)
        return False

    def on_heartbeat(self, req: pb.HeartbeatRequest, context) -> pb.HeartbeatReply:
        node = self.node
        node.recv(req.h)
        if req.h.term < node.term:
            return pb.HeartbeatReply(h=node.hdr(), ok=False)
        from .node import Member
        leader = Member.from_pb(req.leader)
        leader.address = node.fix_address(leader.address, context)
        if node.is_leader and leader.id != node.id:
            if req.h.term == node.term and leader.id < node.id:
                return pb.HeartbeatReply(h=node.hdr(), ok=False)  # eu ganho o desempate
        node.accept_leader(leader, req.h.term)
        members = [Member.from_pb(m) for m in req.members]
        for m in members:
            if m.id == leader.id:
                m.address = leader.address
        node.set_members(members)
        with node.lock:
            node.routes = {r.island_id: (r.node_id, r.address) for r in req.routes}
            node.routes_job = req.job_id
        self.leader_store_version = req.store_version
        self.leader_store_term = req.h.term
        self.leader_seen()
        islands = node.jobs.running_islands_for_heartbeat(req.job_id)
        return pb.HeartbeatReply(h=node.hdr(), ok=True, store_version=node.store.version,
                                 running_islands=islands, job_id=req.job_id)

    # ------------------------------------------------------------ lado do líder

    def on_join(self, req: pb.JoinRequest, context) -> pb.JoinReply:
        node = self.node
        node.recv(req.h)
        from .node import Member
        joiner = Member.from_pb(req.node)
        joiner.address = node.fix_address(joiner.address, context)
        node.add_seed(joiner.address)
        node.add_known(joiner)
        if req.seen_address:
            node.learn_own_address(req.seen_address)
        if not node.is_leader:
            return pb.JoinReply(h=node.hdr(), accepted=False, responder=node.info(),
                                leader_id=node.leader_id or 0, leader_address=node.leader_address or "")
        with node.lock:
            previous = node.members.get(joiner.id)
        restarted = previous is not None and previous.incarnation != joiner.incarnation
        node.add_member(joiner)
        self.last_ok[joiner.id] = time.monotonic()
        self.running.pop(joiner.id, None)
        node.persist_members()
        lam = node.lamport.value
        node.event("member_join", lamport=lam, member=joiner.id, address=joiner.address,
                   incarnation=joiner.incarnation)
        if previous is None or restarted:
            was_here = any(h.get("node") == joiner.id for h in node.store.get("history") or [])
            node.history("join", node=joiner.id, host=joiner.host, address=joiner.address,
                         incarnation=joiner.incarnation)
            what = "voltou (reiniciado)" if restarted or was_here else "entrou no cluster"
            node.cluster_log(f"Nó {joiner.id} ({joiner.host}, {joiner.address}) {what}")
            self.pool.submit(node.berkeley.sync_newcomer, joiner)
            if restarted:
                node.jobs.on_member_restarted(joiner.id)
        return pb.JoinReply(h=node.hdr(), accepted=True, responder=node.info(), leader_id=node.id,
                            leader_address=node.address,
                            members=[m.to_pb() for m in node.member_list()])

    def _leader_tick(self) -> None:
        node = self.node
        members = node.member_list(include_self=False)
        routes = node.jobs.routes()
        job_id = node.jobs.current_job_id() or ""
        member_pb = [m.to_pb() for m in node.member_list()]
        for m in members:
            if m.id in self._hb_inflight:
                continue
            self._hb_inflight.add(m.id)
            self.pool.submit(self._heartbeat, m, member_pb, routes, job_id)
        self._check_failures()

    def _heartbeat(self, m, member_pb, routes, job_id) -> None:
        node = self.node
        try:
            req = pb.HeartbeatRequest(h=node.hdr(), leader=node.info(), members=member_pb, job_id=job_id,
                                      routes=[pb.IslandRoute(island_id=i, node_id=n, address=a)
                                              for i, (n, a) in routes.items()],
                                      store_version=node.store.version)
            rep = node.call(m.address, "Heartbeat", req, timeout=config.HEARTBEAT_RPC_TIMEOUT)
            if rep is not None and rep.ok and node.is_leader:
                self.last_ok[m.id] = time.monotonic()
                self.running[m.id] = (rep.job_id, list(rep.running_islands), time.monotonic())
        finally:
            self._hb_inflight.discard(m.id)

    def _check_failures(self) -> None:
        node = self.node
        now = time.monotonic()
        for m in node.member_list(include_self=False):
            last = self.last_ok.setdefault(m.id, now)
            if now - last > config.MEMBER_FAILURE_TIMEOUT and node.is_leader:
                self.declare_dead(m, now - last)

    def declare_dead(self, m, silent_for: float) -> None:
        node = self.node
        node.remove_member(m.id)
        self.last_ok.pop(m.id, None)
        self.running.pop(m.id, None)
        node.persist_members()
        lam = node.lamport.tick()
        node.event("member_dead", lamport=lam, member=m.id, silent_for=round(silent_for, 2))
        node.history("dead", node=m.id, host=m.host, silent_for=round(silent_for, 2))
        node.cluster_log(f"FALHA DETECTADA: nó {m.id} ({m.host}) sem resposta há {silent_for:.1f}s -> removido")
        node.jobs.reconcile()
