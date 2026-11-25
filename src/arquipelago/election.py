"""Eleição de líder pelo algoritmo do Valentão (Bully), de Garcia-Molina.

1. O nó P que convoca a eleição envia ELEIÇÃO a todos os nós com id maior.
2. Se ninguém responde OK dentro do prazo, P vence: incrementa o mandato
   (term) e envia COORDENADOR a todos.
3. Quem recebe ELEIÇÃO de um id menor responde OK e convoca sua própria
   eleição (se ainda não estiver em uma). Se já for o líder, apenas reenvia
   COORDENADOR ao candidato.
4. Quem recebeu OK espera o COORDENADOR; se ele não chegar a tempo, recomeça.

O mandato acompanha cada mensagem e serve para descartar ordens de um líder
antigo (por exemplo, um líder que ficou isolado e depois voltou).
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from . import config
from .rpc import arquipelago_pb2 as pb

log = logging.getLogger("arquipelago")


class BullyElection:
    def __init__(self, node):
        self.node = node
        self._lock = threading.Lock()
        self._running = False
        self._coordinator = threading.Event()
        self.pool = ThreadPoolExecutor(max_workers=16, thread_name_prefix="bully")
        self.rounds = 0

    @property
    def in_progress(self) -> bool:
        return self._running

    def start(self, reason: str) -> None:
        with self._lock:
            if self._running or not self.node.running:
                return
            self._running = True
        threading.Thread(target=self._run, args=(reason,), name="election", daemon=True).start()

    def leader_known(self) -> None:
        self._coordinator.set()

    def _run(self, reason: str) -> None:
        node = self.node
        try:
            lam = node.lamport.tick()
            node.event("election_start", lamport=lam, reason=reason)
            node.local_log(f"Eleição convocada pelo nó {node.id}: {reason}")
            for _ in range(10):
                if not node.running or node.is_leader:
                    return
                self.rounds += 1
                higher = [m for m in node.known_nodes() if m.id > node.id]
                self._coordinator.clear()
                got_ok = self._send_elections(higher) if higher else False
                if not got_ok:
                    node.become_leader(f"eleição Bully ({reason})")
                    return
                node.local_log("Recebi OK de um nó com id maior; aguardando COORDENADOR")
                if self._coordinator.wait(config.COORDINATOR_WAIT) and node.leader_id is not None:
                    return
                node.local_log("COORDENADOR não chegou a tempo; repetindo a eleição")
        except Exception:
            log.exception("erro na eleição")
        finally:
            self._running = False

    def _send_elections(self, higher) -> bool:
        node = self.node

        def ask(m):
            h = node.hdr()
            msg = f"elec:{node.id}>{m.id}:{h.lamport}"
            node.event("election_send", msg=msg, lamport=h.lamport, to=m.id)
            rep = node.call(m.address, "Election", pb.ElectionRequest(h=h, candidate=node.info()),
                            timeout=config.ELECTION_REPLY_TIMEOUT)
            return rep is not None and rep.ok

        results = list(self.pool.map(ask, higher))
        return any(results)

    def announce_coordinator(self, targets=None) -> None:
        node = self.node
        targets = targets if targets is not None else node.known_nodes()

        def tell(m):
            h = node.hdr()
            msg = f"coord:{node.id}>{m.id}:t{h.term}:{h.lamport}"
            node.event("coordinator_send", msg=msg, lamport=h.lamport, to=m.id, term=h.term)
            node.call(m.address, "Coordinator", pb.CoordinatorAnnounce(h=h, leader=node.info()), timeout=1.0)

        for m in targets:
            self.pool.submit(tell, m)

    # ------------------------------------------------------------ handlers

    def on_election(self, req: pb.ElectionRequest, context) -> pb.ElectionReply:
        node = self.node
        lam = node.recv(req.h)
        from .node import Member
        cand = Member.from_pb(req.candidate)
        cand.address = node.fix_address(cand.address, context)
        node.add_known(cand)
        node.event("election_recv", msg=f"elec:{cand.id}>{node.id}:{req.h.lamport}", lamport=lam,
                   frm=cand.id)
        if cand.id >= node.id:
            return pb.ElectionReply(h=node.hdr(), ok=False)
        if node.is_leader:
            # O candidato está vivo mas não recebia heartbeats: vira membro.
            if cand.id not in node.member_ids():
                node.add_member(cand)
                node.membership.last_ok[cand.id] = time.monotonic()
                node.persist_members()
            self.announce_coordinator([cand])
        else:
            self.start(f"recebi ELEIÇÃO do nó {cand.id}")
        return pb.ElectionReply(h=node.hdr(), ok=True)

    def on_coordinator(self, req: pb.CoordinatorAnnounce, context) -> pb.Ack:
        node = self.node
        lam = node.recv(req.h)
        from .node import Member
        leader = Member.from_pb(req.leader)
        leader.address = node.fix_address(leader.address, context)
        node.event("coordinator_recv", msg=f"coord:{leader.id}>{node.id}:t{req.h.term}:{req.h.lamport}",
                   lamport=lam, leader=leader.id, term=req.h.term)
        if req.h.term < node.term:
            return pb.Ack(h=node.hdr(), ok=False, info="mandato antigo")
        if node.is_leader and leader.id != node.id and req.h.term == node.term and leader.id < node.id:
            return pb.Ack(h=node.hdr(), ok=False, info="desempate: id maior")
        node.accept_leader(leader, req.h.term)
        return pb.Ack(h=node.hdr(), ok=True)
