"""Nó do Arquipélago: identidade, papel (líder/seguidor), membros e chamadas RPC.

Todos os nós são iguais; o líder (coordenador) é escolhido pelo Bully. Os
componentes ficam em módulos separados e recebem o nó como contexto:

  membership.py   entrada no cluster, heartbeats e detecção de falhas
  election.py     eleição de líder (Bully)
  replication.py  replicação primário-backup do estado do coordenador
  berkeley.py     sincronização de relógios físicos (Berkeley)
  jobs.py         criação de jobs, distribuição/reatribuição de ilhas
  island.py       execução de uma ilha do AG + migração
  orchestrator.py bateria de testes automática
"""

import logging
import os
import socket
import threading
import time
from collections import deque
from concurrent import futures
from dataclasses import dataclass

import grpc

from . import config
from .berkeley import BerkeleySync
from .clocks import LamportClock, SyncedClock
from .discovery import LanDiscovery
from .election import BullyElection
from . import events as events_mod
from .events import EventLog
from .jobs import JobManager
from .membership import Membership
from .netutil import fix_address, primary_ip
from .orchestrator import TestOrchestrator
from .replication import Replicator
from .rpc import arquipelago_pb2 as pb
from .rpc import arquipelago_pb2_grpc as pbg
from .servicer import NodeServicer
from .store import ReplicatedStore
from .sysinfo import machine_info

log = logging.getLogger("arquipelago")

RETRYABLE = (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)


@dataclass
class Member:
    id: int
    address: str
    incarnation: int = 0
    host: str = ""
    machine: str = ""

    def to_pb(self) -> pb.NodeInfo:
        return pb.NodeInfo(id=self.id, address=self.address, incarnation=self.incarnation, host=self.host,
                           machine=self.machine)

    @staticmethod
    def from_pb(n: pb.NodeInfo) -> "Member":
        return Member(n.id, n.address, n.incarnation, n.host, n.machine)

    def to_dict(self) -> dict:
        return {"id": self.id, "address": self.address, "incarnation": self.incarnation, "host": self.host,
                "machine": self.machine}


class Node:
    def __init__(self, node_id: int, port: int = config.DEFAULT_GRPC_PORT, advertise: str | None = None,
                 peers=(), data_dir: str = "dados", skew: float = 0.0, incarnation: int = 0,
                 preempt: bool = True, crash_mode: str = "exit", lan: bool = False,
                 cluster: str = config.DEFAULT_CLUSTER, bind: str = "0.0.0.0",
                 auto_clock_sync: bool = True):
        self.id = int(node_id)
        self.port = int(port)
        self.hostname = socket.gethostname()
        self.machine = machine_info()
        self.address = f"{advertise or primary_ip()}:{self.port}"
        self.incarnation = int(incarnation)
        self.preempt = preempt
        self.crash_mode = crash_mode
        self.lan = lan
        self.cluster = cluster
        self.bind = bind
        self.auto_clock_sync = auto_clock_sync  # False: 1ª rodada de Berkeley só quando pedida (teste)
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

        self.lock = threading.RLock()
        self.running = False
        self.started_at = time.time()
        self.role = "follower"
        self.term = 0
        self.leader_id: int | None = None
        self.leader_address: str | None = None
        self.members: dict[int, Member] = {self.id: self.self_member()}
        self.known: dict[int, Member] = {}
        self.seeds: set[str] = {p for p in peers if p}
        self.routes: dict[int, tuple[int, str]] = {}
        self.routes_job = ""

        # O relógio de Lamport sobrevive a reinícios: é restaurado do log em disco.
        if self.incarnation == 0:
            events_mod.clear_previous(data_dir, self.id)
        self.lamport = LamportClock(events_mod.max_lamport(data_dir, self.id))
        self.clock = SyncedClock(skew)
        self.store = ReplicatedStore()
        self.events = EventLog(self.id, os.path.join(data_dir, f"eventos_no{self.id}_inc{self.incarnation}.jsonl"))
        self.local_lines = deque(maxlen=300)

        self._stubs: dict[str, pbg.ArquipelagoNodeStub] = {}
        self._channels: dict[str, grpc.Channel] = {}
        self._stub_lock = threading.Lock()
        self.server = None
        self.discovery = None

        self.membership = Membership(self)
        self.election = BullyElection(self)
        self.replicator = Replicator(self)
        self.berkeley = BerkeleySync(self)
        self.jobs = JobManager(self)
        self.orchestrator = TestOrchestrator(self)
        self.servicer = NodeServicer(self)

    # ------------------------------------------------------------ ciclo de vida

    def start(self) -> None:
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=48),
                                  options=config.GRPC_CHANNEL_OPTIONS)
        pbg.add_ArquipelagoNodeServicer_to_server(self.servicer, self.server)
        bound = self.server.add_insecure_port(f"{self.bind}:{self.port}")
        if not bound:
            raise OSError(f"não foi possível abrir a porta {self.port}")
        self.server.start()
        self.running = True
        self.membership.leaderless_since = time.monotonic()
        for target, name in ((self.membership.run, "membership"), (self.replicator.run, "replicator"),
                             (self.jobs.run, "jobs"), (self.berkeley.run, "berkeley"),
                             (self.orchestrator.run, "orchestrator")):
            threading.Thread(target=target, name=name, daemon=True).start()
        if self.lan:
            self.discovery = LanDiscovery(self)
            self.discovery.start()
        self.event("node_start", address=self.address, incarnation=self.incarnation)
        self.local_log(f"Nó {self.id} iniciado em {self.address} (encarnação {self.incarnation})")

    def stop(self) -> None:
        """Parada "limpa" (usada nos testes automatizados em um único processo)."""
        self.running = False
        self.jobs.stop_all()
        if self.discovery:
            self.discovery.stop()
        if self.server:
            self.server.stop(0)
        with self._stub_lock:
            for ch in self._channels.values():
                ch.close()
            self._channels.clear()
            self._stubs.clear()

    def crash(self, reason: str) -> None:
        """Falha simulada por parada abrupta (crash-stop): o processo morre na hora."""
        self.event("crash", reason=reason)
        log.warning("FALHA SIMULADA: %s", reason)
        if self.crash_mode == "exit":
            logging.shutdown()
            os._exit(config.CRASH_EXIT_CODE)
        self.stop()

    # ------------------------------------------------------------ identidade / papel

    @property
    def is_leader(self) -> bool:
        return self.role == "leader"

    def self_member(self) -> Member:
        return Member(self.id, self.address, self.incarnation, self.hostname, self.machine)

    def info(self) -> pb.NodeInfo:
        return self.self_member().to_pb()

    def observe_term(self, term: int) -> None:
        if term <= self.term:
            return
        with self.lock:
            if term <= self.term:
                return
            self.term = term
            was_leader = self.is_leader
        if was_leader:
            self.step_down(f"mandato {term} maior que o meu foi observado")

    def accept_leader(self, leader: Member, term: int) -> None:
        with self.lock:
            if term < self.term:
                return
            if self.is_leader and leader.id != self.id:
                self.role = "follower"
                self.local_log(f"Deixando a liderança para o nó {leader.id}")
            changed = self.leader_id != leader.id or self.term != term
            self.term = term
            self.leader_id = leader.id
            self.leader_address = leader.address
            self.members[leader.id] = leader
            self.known[leader.id] = leader
        self.membership.leader_seen()
        if changed:
            lam = self.lamport.tick()
            self.event("leader_accepted", lamport=lam, leader=leader.id, term=term)
            self.local_log(f"Líder atual: nó {leader.id} ({leader.address}), mandato {term}")
        self.election.leader_known()

    def become_leader(self, reason: str) -> None:
        with self.lock:
            if self.is_leader:
                return
            new_term = max(self.term, self.store.term) + 1
            self.term = new_term
            self.role = "leader"
            self.leader_id = self.id
            self.leader_address = self.address
            # Todos os nós conhecidos recebem o COORDENADOR e passam a ser membros;
            # os que estiverem mortos serão removidos pelo detector de falhas.
            for m in self.known.values():
                self.members.setdefault(m.id, m)
            self.members[self.id] = self.self_member()
        self.store.start_term(new_term)
        self.replicator.reset()
        self.membership.on_become_leader()
        self.persist_members()
        self.store.put("leader", {"id": self.id, "address": self.address, "term": new_term,
                                  "host": self.hostname, "since": self.clock.now()})
        self.history("leader", node=self.id, term=new_term, reason=reason)
        lam = self.lamport.tick()
        self.event("leader_elected", lamport=lam, term=new_term, reason=reason)
        self.cluster_log(f"Nó {self.id} ({self.hostname}) é o LÍDER (mandato {new_term}) — {reason}")
        self.election.announce_coordinator()
        self.berkeley.request_round()

    def step_down(self, reason: str) -> None:
        with self.lock:
            if not self.is_leader:
                return
            self.role = "follower"
            self.leader_id = None
            self.leader_address = None
        self.membership.leaderless_since = time.monotonic()
        lam = self.lamport.tick()
        self.event("step_down", lamport=lam, reason=reason)
        self.local_log(f"Deixei de ser líder: {reason}")

    def forget_leader(self) -> int | None:
        with self.lock:
            old = self.leader_id
            self.leader_id = None
            self.leader_address = None
            if old is not None and old != self.id:
                self.members.pop(old, None)
                self.known.pop(old, None)
        self.membership.leaderless_since = time.monotonic()
        return old

    # ------------------------------------------------------------ membros

    def member_list(self, include_self: bool = True) -> list[Member]:
        with self.lock:
            return [m for mid, m in sorted(self.members.items()) if include_self or mid != self.id]

    def member_ids(self) -> list[int]:
        with self.lock:
            return sorted(self.members)

    def set_members(self, members: list[Member]) -> None:
        with self.lock:
            self.members = {m.id: m for m in members}
            self.members[self.id] = self.self_member()
            for m in members:
                self.known[m.id] = m

    def add_member(self, m: Member) -> None:
        with self.lock:
            self.members[m.id] = m
            self.known[m.id] = m

    def remove_member(self, node_id: int) -> None:
        with self.lock:
            self.members.pop(node_id, None)
            self.known.pop(node_id, None)

    def add_known(self, m: Member) -> None:
        if m.id and m.id != self.id:
            with self.lock:
                self.known[m.id] = m

    def add_seed(self, address: str) -> None:
        if address and address != self.address:
            with self.lock:
                self.seeds.add(address)

    def known_nodes(self) -> list[Member]:
        with self.lock:
            merged = dict(self.known)
            merged.update(self.members)
            merged.pop(self.id, None)
            return list(merged.values())

    def persist_members(self) -> None:
        if self.is_leader:
            self.store.put("members", [m.to_dict() for m in self.member_list()])

    def learn_own_address(self, seen_address: str) -> None:
        """Outro nó nos alcançou por `seen_address`: esse é nosso endereço "de fora"."""
        if not seen_address or seen_address == self.address:
            return
        host = seen_address.rpartition(":")[0]
        if host.startswith("127.") or host == "localhost":
            return
        old = self.address
        self.address = f"{host}:{self.port}"
        with self.lock:
            self.members[self.id] = self.self_member()
            if self.leader_id == self.id:
                self.leader_address = self.address
        self.local_log(f"Endereço corrigido: {old} -> {self.address}")

    # ------------------------------------------------------------ mensagens

    def hdr(self) -> pb.Header:
        """Cabeçalho de envio: incrementa Lamport (evento de envio)."""
        return pb.Header(lamport=self.lamport.tick(), sender_id=self.id, term=self.term,
                         phys_time=self.clock.now())

    def recv(self, h: pb.Header) -> int:
        """Recebimento: atualiza Lamport e observa o mandato do remetente."""
        lam = self.lamport.receive(h.lamport)
        self.observe_term(h.term)
        return lam

    def stub(self, address: str) -> pbg.ArquipelagoNodeStub:
        with self._stub_lock:
            s = self._stubs.get(address)
            if s is None:
                ch = grpc.insecure_channel(address, options=config.GRPC_CHANNEL_OPTIONS)
                self._channels[address] = ch
                s = self._stubs[address] = pbg.ArquipelagoNodeStub(ch)
            return s

    def call(self, address: str, method: str, request, timeout: float = config.RPC_TIMEOUT,
             retries: int = 0):
        """Chamada gRPC com timeout e novas tentativas (backoff exponencial).

        Devolve a resposta ou None se o destino estiver inalcançável.
        """
        for attempt in range(retries + 1):
            try:
                reply = getattr(self.stub(address), method)(request, timeout=timeout)
                h = getattr(reply, "h", None)
                if h is not None and h.lamport:
                    self.recv(h)
                return reply
            except grpc.RpcError as e:
                code = e.code() if hasattr(e, "code") else None
                if code not in RETRYABLE or attempt == retries or not self.running:
                    log.debug("RPC %s -> %s falhou: %s", method, address, code)
                    return None
                time.sleep(0.2 * (2 ** attempt))
        return None

    def send(self, target_id: int, address: str, method: str, request, timeout=config.RPC_TIMEOUT,
             retries: int = 0):
        """Como call(), mas entrega localmente quando o destino é o próprio nó."""
        if target_id == self.id:
            try:
                return getattr(self.servicer, method)(request, None)
            except Exception:
                log.exception("entrega local de %s falhou", method)
                return None
        return self.call(address, method, request, timeout, retries)

    def send_to_leader(self, method: str, request, timeout=config.RPC_TIMEOUT, retries: int = 0):
        with self.lock:
            lid, addr = self.leader_id, self.leader_address
        if lid is None:
            return None
        return self.send(lid, addr, method, request, timeout, retries)

    def fix_address(self, advertised: str, context) -> str:
        peer = context.peer() if context is not None else None
        return fix_address(advertised, peer)

    # ------------------------------------------------------------ logs

    def event(self, kind: str, msg: str = "", lamport: int | None = None, **data) -> None:
        lam = self.lamport.value if lamport is None else lamport
        self.events.record(lam, self.clock.now(), kind, msg, **data)

    def local_log(self, text: str) -> None:
        self.local_lines.append({"t": self.clock.now(), "text": text})
        log.info(text)

    def cluster_log(self, text: str) -> None:
        """Linha do log do cluster: replicada para todos os nós (aparece em todas as telas)."""
        if not self.is_leader:
            self.local_log(text)
            return
        log.info("[cluster] %s", text)
        with self.store.lock:
            seq = int(self.store.get("log/seq", 0)) + 1
            self.store.put("log/seq", seq)
            self.store.put(f"log/{seq:08d}", {"t": self.clock.now(), "node": self.id, "text": text})
            old = seq - config.LOG_LINES_KEPT
            if old > 0:
                self.store.delete(f"log/{old:08d}")

    def history(self, kind: str, **data) -> None:
        """Histórico de eventos do cluster (entradas/saídas/líderes), usado no relatório do teste."""
        if not self.is_leader:
            return
        entry = {"kind": kind, "t": self.clock.now(), **data}
        self.store.update("history", lambda h: (h or [])[-299:] + [entry])

    # ------------------------------------------------------------ status

    def status(self) -> dict:
        with self.lock:
            members = [m.to_dict() for m in self.member_list()]
            leader = self.leader_id
            leader_addr = self.leader_address
        cluster_log = [v for _, v in self.store.items("log/0")]
        # O hash percorre o estado inteiro (com as populações): só é calculado
        # fora de jobs, quando a fase de consistência o usa, para não roubar CPU das ilhas.
        digest = "" if self.jobs.current_job_id() else self.store.digest()
        return {
            "id": self.id, "address": self.address, "host": self.hostname, "incarnation": self.incarnation,
            "role": self.role, "leader_id": leader, "leader_address": leader_addr, "term": self.term,
            "members": members, "lamport": self.lamport.value,
            "clock": {"skew": self.clock.skew, "offset": self.clock.offset, "now": self.clock.now()},
            "store": {"term": self.store.term, "version": self.store.version, "digest": digest},
            "running_islands": self.jobs.running_islands(),
            "job": self.jobs.summary(),
            "test": self.store.get("test"),
            "cluster_log": cluster_log[-80:],
            "local_log": list(self.local_lines)[-40:],
            "uptime": time.time() - self.started_at,
        }
