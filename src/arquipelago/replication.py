"""Controle de réplicas: replicação primário-backup do estado do coordenador.

O líder (primário) é o único que escreve no ReplicatedStore. Este módulo
propaga as escritas para todos os seguidores (backups):

  * cada seguidor tem uma "versão confirmada" (term, versão);
  * se está no mesmo mandato, recebe só o delta desde essa versão;
  * se é novo, reiniciou ou o líder mudou, recebe um snapshot completo;
  * um seguidor recusa deltas cuja base não coincide com sua versão (pede
    snapshot), então nenhuma réplica pula estados.

Escritas críticas (antes de injetar uma falha, ao concluir um job) usam
wait_synced(): o líder só segue depois que todos os backups vivos confirmaram
(replicação síncrona). As demais são assíncronas (enviadas em lote a cada
REPLICATION_INTERVAL), para não atrasar o cálculo.

Quando o líder cai, o novo líder (eleito pelo Bully) já tem uma cópia do
estado: lista de ilhas, checkpoints das populações, melhor solução e o
andamento da bateria de testes. Ele continua a partir dali.
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from . import config
from .rpc import arquipelago_pb2 as pb

log = logging.getLogger("arquipelago")


class Replicator:
    def __init__(self, node):
        self.node = node
        self.pool = ThreadPoolExecutor(max_workers=12, thread_name_prefix="repl")
        self.acked: dict[int, tuple[int, int]] = {}  # membro -> (term, versão confirmada)
        self._inflight: set[int] = set()
        self._lock = threading.Lock()
        self.full_syncs = 0
        self.deltas = 0
        self.bytes_sent = 0

    def reset(self) -> None:
        with self._lock:
            self.acked.clear()

    def run(self) -> None:
        node = self.node
        store = node.store
        while node.running:
            try:
                if node.is_leader:
                    self._round()
            except Exception:
                log.exception("erro no replicador")
            with store.changed:
                store.changed.wait(config.REPLICATION_INTERVAL)

    def _round(self) -> None:
        """Uma rodada: codifica cada delta UMA vez por versão-base e envia a todos que precisam.

        Os seguidores em dia costumam estar na mesma versão, então um único
        JSON+zlib serve a todos.
        """
        node = self.node
        store = node.store
        pending = []
        for m in node.member_list(include_self=False):
            with self._lock:
                if m.id in self._inflight:
                    continue
                term, ver = self.acked.get(m.id, (-1, 0))
                if term == store.term and ver >= store.version:
                    continue
                self._inflight.add(m.id)
            full = term != store.term or ver <= 0
            pending.append((m, 0 if full else ver, full))
        if not pending:
            return
        encoded = {}
        with store.lock:  # todas as codificações desta rodada veem a mesma versão
            for _, base, _ in pending:
                if base not in encoded:
                    encoded[base] = store.encode_since(base)
        for m, base, full in pending:
            self.pool.submit(self._push, m, base, full, encoded[base])

    def _push(self, m, base: int, full: bool, encoded) -> None:
        node = self.node
        try:
            data, sterm, sver = encoded
            req = pb.ReplicationRequest(h=node.hdr(), base_version=base, version=sver, full=full, delta=data)
            rep = node.call(m.address, "Replicate", req, timeout=5.0 if full else 2.0)
            if rep is None:
                return
            self.bytes_sent += len(data)
            with self._lock:
                if rep.ok:
                    self.acked[m.id] = (sterm, rep.version)
                    if full:
                        self.full_syncs += 1
                        node.event("replication_full", member=m.id, version=rep.version, bytes=len(data))
                    else:
                        self.deltas += 1
                elif rep.need_full:
                    self.acked[m.id] = (-1, 0)
        except Exception:
            log.exception("erro ao replicar para %s", m.id)
        finally:
            with self._lock:
                self._inflight.discard(m.id)

    def wait_synced(self, timeout: float = 3.0) -> tuple[list[int], list[int]]:
        """Replicação síncrona: espera todos os membros vivos confirmarem a versão atual."""
        node = self.node
        target_term, target = node.store.term, node.store.version
        deadline = time.monotonic() + timeout
        while True:
            pending, done = [], []
            for m in node.member_list(include_self=False):
                term, ver = self.acked.get(m.id, (-1, -1))
                (done if term == target_term and ver >= target else pending).append(m.id)
            if not pending or time.monotonic() > deadline or not node.is_leader:
                return done, pending
            with node.store.changed:
                node.store.changed.notify_all()  # acorda o replicador
            time.sleep(0.05)

    # ------------------------------------------------------------ seguidor

    def on_replicate(self, req: pb.ReplicationRequest, context) -> pb.ReplicationReply:
        node = self.node
        node.recv(req.h)
        if req.h.term < node.term or (node.is_leader and req.h.sender_id != node.id):
            return pb.ReplicationReply(h=node.hdr(), ok=False, version=node.store.version)
        ok, version = node.store.apply(req.delta, req.full, req.base_version)
        return pb.ReplicationReply(h=node.hdr(), ok=ok, version=version, need_full=not ok)
