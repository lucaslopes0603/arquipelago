"""Execução de uma ilha do AG dentro de um nó trabalhador.

Ciclo de cada época:
  1. incorpora os imigrantes recebidos (substituem os piores indivíduos);
  2. evolui `gens_per_epoch` gerações;
  3. migra: envia os melhores indivíduos à próxima ilha do anel
     (ilha i -> ilha (i+1) mod k), diretamente para o nó que a executa;
  4. reporta o progresso ao líder, incluindo o checkpoint da população.

Migrações e relatórios intermediários são "melhor esforço": se o destino
falhar, a ilha segue calculando (o próximo relatório leva um checkpoint mais
novo). Só o relatório final é repetido até ser confirmado.
"""

import logging
import queue
import threading
import time

import numpy as np

from . import ga
from .rpc import arquipelago_pb2 as pb

log = logging.getLogger("arquipelago")


class IslandRunner(threading.Thread):
    def __init__(self, node, job_id: str, island_id: int, spec: dict, start_epoch: int,
                 population: np.ndarray | None, dist: np.ndarray):
        super().__init__(name=f"ilha-{island_id}", daemon=True)
        self.node = node
        self.job_id = job_id
        self.island_id = island_id
        self.spec = spec
        self.start_epoch = start_epoch
        self.population = population
        self.dist = dist
        self.epoch = start_epoch
        self.epochs = int(spec["epochs"])
        self.best: float | None = None
        self.migrations_sent = 0
        self.migrations_failed = 0
        self.immigrants_received = 0
        self._halt = threading.Event()
        self._inbox: queue.Queue = queue.Queue()

    def stop(self) -> None:
        self._halt.set()

    @property
    def halted(self) -> bool:
        return self._halt.is_set() or not self.node.running

    def push_immigrants(self, tours: np.ndarray) -> None:
        self._inbox.put(tours)

    def _drain(self):
        batches = []
        while True:
            try:
                batches.append(self._inbox.get_nowait())
            except queue.Empty:
                break
        if not batches:
            return None
        self.immigrants_received += sum(len(b) for b in batches)
        return np.vstack(batches)

    def run(self) -> None:
        try:
            island = ga.IslandGA(self.spec, self.island_id, self.population, self.dist)
            self.best = island.best_length()
            for epoch in range(self.start_epoch, self.epochs):
                if self.halted:
                    return
                island.integrate(self._drain())
                self.best = island.run_epoch(epoch)
                self.epoch = epoch + 1
                self._migrate(island, epoch)
                if not self._report(island, final=self.epoch >= self.epochs):
                    return
            if self.start_epoch >= self.epochs:
                self._report(island, final=True)
        except Exception:
            log.exception("erro na ilha %s", self.island_id)

    def _migrate(self, island: ga.IslandGA, epoch: int) -> None:
        k = int(self.spec["islands"])
        if k < 2:
            return
        node = self.node
        to = (self.island_id + 1) % k
        route = node.jobs.route_for(self.job_id, to)
        if route is None:
            self.migrations_failed += 1
            return
        owner, address = route
        h = node.hdr()
        msg_id = f"migr:{self.job_id}:{self.island_id}>{to}:e{epoch}:n{node.id}:{h.lamport}"
        node.event("migr_send", msg=msg_id, lamport=h.lamport, frm=self.island_id, to=to, epoch=epoch,
                   dest=owner)
        req = pb.Migration(h=h, job_id=self.job_id, msg_id=msg_id, from_island=self.island_id,
                           to_island=to, epoch=epoch, n_cities=island.n_cities,
                           tours=ga.pack(island.migrants(int(self.spec["migrants"]))))
        rep = node.send(owner, address, "Migrate", req, timeout=1.0)
        if rep is not None and rep.ok:
            self.migrations_sent += 1
        else:
            self.migrations_failed += 1

    def _report(self, island: ga.IslandGA, final: bool) -> bool:
        """Envia progresso + checkpoint ao líder. Retorna False se a ilha deve parar."""
        node = self.node
        deadline = time.monotonic() + 180.0
        while True:
            req = pb.ProgressReport(
                h=node.hdr(), job_id=self.job_id, island_id=self.island_id, epoch=self.epoch,
                best_length=island.best_length(), best_tour=ga.pack(island.best_tour()),
                population=ga.pack(island.pop), finished=final,
                compute_seconds=island.compute_seconds, generations=island.generations)
            rep = node.send_to_leader("ReportProgress", req, timeout=3.0)
            if rep is not None and rep.ok:
                if final:
                    node.jobs.mark_completed(self.job_id, self.island_id, req)
                    node.event("island_done", island=self.island_id, job=self.job_id,
                               best=round(island.best_length(), 2))
                return True
            if rep is not None and rep.info in ("not_owner", "job_unknown"):
                node.local_log(f"Ilha {self.island_id}: líder respondeu '{rep.info}' -> parando esta cópia")
                return False
            if not final:
                return True
            if self.halted or time.monotonic() > deadline:
                return False
            time.sleep(0.5)
