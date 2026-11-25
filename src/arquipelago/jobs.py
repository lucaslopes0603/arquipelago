"""Jobs do AG: criação, distribuição de ilhas, checkpoints e reatribuição.

Estado do job no ReplicatedStore (replicado em todos os nós):
  job/current                 id do job em execução ("" = nenhum)
  job/<id>/spec               parâmetros + coordenadas das cidades
  job/<id>/meta               status, tempos, melhor solução global, reatribuições
  job/<id>/island/<i>         dono, época, melhor rota e checkpoint da população

Tratamento de falhas (lado do líder, laço reconcile a cada 0,5 s):
  * ilha cujo dono saiu da lista de membros -> reatribuída ao nó vivo menos
    carregado, que retoma do último checkpoint (perde-se no máximo uma época);
  * dono vivo que não está executando a ilha (ex.: reiniciou) -> reenvio.
Relatórios de um nó que não é mais o dono da ilha são recusados ("not_owner"),
o que impede duas cópias da mesma ilha após uma falsa suspeita de falha.
"""

import json
import logging
import threading
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import numpy as np

from . import ga
from .island import IslandRunner
from .rpc import arquipelago_pb2 as pb
from .store import b64, unb64
from .tsp import distance_matrix

log = logging.getLogger("arquipelago")

REDISPATCH_AFTER = 3.0


def island_key(jid: str, iid: int) -> str:
    return f"job/{jid}/island/{iid:03d}"


class JobManager:
    def __init__(self, node):
        self.node = node
        self.lock = threading.RLock()
        self.pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="jobs")
        self.runners: dict[tuple[str, int], IslandRunner] = {}
        self.completed: dict[tuple[str, int], pb.ProgressReport] = {}
        self.dispatched: dict[tuple[str, int], float] = {}
        self._dist_cache: dict[str, np.ndarray] = {}

    # ------------------------------------------------------------ leitura do estado

    @property
    def store(self):
        return self.node.store

    def current_job_id(self) -> str | None:
        return self.store.get("job/current") or None

    def meta(self, jid: str) -> dict | None:
        return self.store.get(f"job/{jid}/meta")

    def spec(self, jid: str) -> dict | None:
        return self.store.get(f"job/{jid}/spec")

    def islands(self, jid: str) -> list[dict]:
        return [v for _, v in self.store.items(f"job/{jid}/island/")]

    def routes(self) -> dict[int, tuple[int, str]]:
        jid = self.current_job_id()
        if not jid:
            return {}
        members = {m.id: m for m in self.node.member_list()}
        return {rec["island"]: (rec["owner"], members[rec["owner"]].address)
                for rec in self.islands(jid) if rec["owner"] in members}

    def route_for(self, jid: str, iid: int):
        node = self.node
        if node.is_leader:
            return self.routes().get(iid) if self.current_job_id() == jid else None
        with node.lock:
            return node.routes.get(iid) if node.routes_job == jid else None

    # ------------------------------------------------------------ líder

    def submit(self, spec: dict) -> tuple[bool, str]:
        node = self.node
        if not node.is_leader:
            return False, "este nó não é o líder"
        with self.lock:
            cur = self.current_job_id()
            if cur:
                return False, f"o job {cur} ainda está em execução"
            alive = node.member_ids()
            pin = int(spec.get("pin_to") or 0)
            owners = [pin] if pin and pin in alive else alive
            k = int(spec.get("islands") or 0) or len(alive)
            spec = dict(ga.DEFAULT_SPEC, **spec)
            spec["islands"] = k
            jid = f"{spec.get('label', 'job')}-{uuid.uuid4().hex[:6]}"
            now = node.clock.now()
            self.store.put(f"job/{jid}/spec", spec)
            for iid in range(k):
                self.store.put(island_key(jid, iid), {
                    "island": iid, "owner": owners[iid % len(owners)], "epoch": 0, "best_length": None,
                    "best_tour": None, "population": "", "finished": False, "compute_seconds": 0.0,
                    "generations": 0, "assigned_at": now, "history": []})
            self.store.put(f"job/{jid}/meta", {
                "id": jid, "label": spec.get("label"), "status": "running", "n_islands": k,
                "n_cities": len(spec["coords"]), "pop_size": spec["pop_size"], "epochs": spec["epochs"],
                "gens_per_epoch": spec["gens_per_epoch"], "nodes": owners, "created_at": now,
                "started_at": now, "finished_at": None, "leader_at_start": node.id,
                "best_length": None, "best_tour": None, "best_island": None, "reassignments": []})
            self.store.put("job/current", jid)
        lam = node.lamport.tick()
        node.event("job_submit", lamport=lam, job=jid, islands=k)
        node.cluster_log(f"Job {jid}: {len(spec['coords'])} cidades, {k} ilha(s) em {len(owners)} nó(s), "
                         f"população {spec['pop_size']}, {spec['epochs']} épocas x {spec['gens_per_epoch']} gerações")
        self.reconcile()
        return True, jid

    def run(self) -> None:
        while self.node.running:
            try:
                if self.node.is_leader:
                    self.reconcile()
            except Exception:
                log.exception("erro no reconcile")
            time.sleep(0.5)

    def on_member_restarted(self, node_id: int) -> None:
        """Dono de ilhas reiniciou (perdeu a memória): as ilhas voltam do checkpoint replicado."""
        node = self.node
        with self.lock:
            jid = self.current_job_id()
            meta = self.meta(jid) if jid else None
            if not meta or meta["status"] != "running":
                return
            spec = self.spec(jid)
            for rec in self.islands(jid):
                if rec["finished"] or rec["owner"] != node_id:
                    continue
                now = node.clock.now()
                info = {"island": rec["island"], "from": node_id, "to": node_id, "epoch": rec["epoch"], "t": now,
                        "reason": "reinício"}
                self.store.update(f"job/{jid}/meta", lambda m: dict(m, reassignments=m["reassignments"] + [info]))
                node.cluster_log(f"Ilha {rec['island']}: nó {node_id} reiniciou -> ilha retomada do checkpoint "
                                 f"da época {rec['epoch']}/{spec['epochs']}")
                self._dispatch(jid, rec, spec)

    def _is_running(self, owner: int, jid: str, iid: int) -> bool:
        if owner == self.node.id:
            r = self.runners.get((jid, iid))
            return (r is not None and r.is_alive()) or (jid, iid) in self.completed
        info = self.node.membership.running.get(owner)
        return info is not None and info[0] == jid and iid in info[1]

    def reconcile(self) -> None:
        node = self.node
        if not node.is_leader:
            return
        with self.lock:
            jid = self.current_job_id()
            meta = self.meta(jid) if jid else None
            if not meta or meta["status"] != "running":
                return
            spec = self.spec(jid)
            alive = set(node.member_ids())
            recs = self.islands(jid)
            load = Counter(r["owner"] for r in recs if not r["finished"] and r["owner"] in alive)
            for rec in recs:
                if rec["finished"]:
                    continue
                iid, owner = rec["island"], rec["owner"]
                if owner not in alive:
                    pin = int(spec.get("pin_to") or 0)
                    candidates = [pin] if pin in alive else sorted(alive)
                    new = min(candidates, key=lambda n: (load[n], n))
                    load[new] += 1
                    now = node.clock.now()
                    rec = dict(rec, owner=new, assigned_at=now)
                    self.store.put(island_key(jid, iid), rec)
                    info = {"island": iid, "from": owner, "to": new, "epoch": rec["epoch"], "t": now,
                            "reason": "falha"}
                    self.store.update(f"job/{jid}/meta",
                                      lambda m: dict(m, reassignments=m["reassignments"] + [info]))
                    lam = node.lamport.tick()
                    node.event("island_reassign", lamport=lam, job=jid, island=iid, frm=owner, to=new,
                               epoch=rec["epoch"])
                    node.cluster_log(f"Ilha {iid}: nó {owner} falhou -> reatribuída ao nó {new}, "
                                     f"retomando do checkpoint da época {rec['epoch']}/{spec['epochs']}")
                    self._dispatch(jid, rec, spec)
                elif not self._is_running(owner, jid, iid):
                    if time.monotonic() - self.dispatched.get((jid, iid), 0.0) > REDISPATCH_AFTER:
                        self._dispatch(jid, rec, spec)

    def _dispatch(self, jid: str, rec: dict, spec: dict) -> None:
        node = self.node
        owner = rec["owner"]
        members = {m.id: m for m in node.member_list()}
        if owner not in members:
            return
        iid = rec["island"]
        h = node.hdr()
        req = pb.IslandAssignment(h=h, job_id=jid, island_id=iid, spec_json=json.dumps(spec).encode(),
                                  start_epoch=int(rec["epoch"]), population=unb64(rec["population"]))
        self.dispatched[(jid, iid)] = time.monotonic()
        node.event("assign_send", msg=f"assign:{jid}:{iid}:{h.lamport}", lamport=h.lamport, island=iid,
                   to=owner, epoch=rec["epoch"])
        self.pool.submit(node.send, owner, members[owner].address, "AssignIsland", req, 5.0, 1)

    def on_report(self, req: pb.ProgressReport, context) -> pb.Ack:
        node = self.node
        node.recv(req.h)
        if not node.is_leader:
            return pb.Ack(h=node.hdr(), ok=False, info="not_leader")
        jid = req.job_id
        key = island_key(jid, req.island_id)
        with self.lock:
            meta = self.meta(jid)
            rec = self.store.get(key)
            if meta is None or rec is None:
                return pb.Ack(h=node.hdr(), ok=False, info="job_unknown")
            if meta["status"] != "running":
                return pb.Ack(h=node.hdr(), ok=True, info="job_done")
            if rec["owner"] != req.h.sender_id:
                return pb.Ack(h=node.hdr(), ok=False, info="not_owner")
            if req.epoch < rec["epoch"] or (req.epoch == rec["epoch"] and rec["finished"]):
                return pb.Ack(h=node.hdr(), ok=True, info="stale")
            best_tour = np.frombuffer(req.best_tour, dtype="<i2").tolist()
            rec = dict(rec, epoch=req.epoch, best_length=req.best_length, best_tour=best_tour,
                       population=b64(req.population), finished=req.finished,
                       compute_seconds=req.compute_seconds, generations=req.generations,
                       history=rec["history"] + [[req.epoch, round(req.best_length, 2)]])
            self.store.put(key, rec)
            if meta["best_length"] is None or req.best_length < meta["best_length"] - 1e-9:
                meta = dict(meta, best_length=req.best_length, best_tour=best_tour, best_island=req.island_id)
                self.store.put(f"job/{jid}/meta", meta)
            if req.finished and all(r["finished"] for r in self.islands(jid)):
                self._finish(jid)
        return pb.Ack(h=node.hdr(), ok=True)

    def _finish(self, jid: str) -> None:
        node = self.node
        now = node.clock.now()
        recs = self.islands(jid)
        meta = self.meta(jid)
        meta = dict(meta, status="done", finished_at=now, wall_seconds=now - meta["started_at"],
                    total_generations=sum(r["generations"] for r in recs),
                    islands_summary=[{"island": r["island"], "owner": r["owner"], "best_length": r["best_length"],
                                      "compute_seconds": r["compute_seconds"]} for r in recs],
                    leader_at_end=node.id)
        self.store.put(f"job/{jid}/meta", meta)
        for r in recs:  # checkpoints não são mais necessários: mantém o estado replicado pequeno
            self.store.put(island_key(jid, r["island"]), dict(r, population=""))
        self.store.put("job/current", "")
        self.store.update("jobs/history", lambda h: (h or [])[-49:] + [jid])
        self.dispatched = {k: v for k, v in self.dispatched.items() if k[0] != jid}
        lam = node.lamport.tick()
        node.event("job_done", lamport=lam, job=jid, best=round(meta["best_length"], 2),
                   wall=round(meta["wall_seconds"], 3))
        node.cluster_log(f"Job {jid} CONCLUÍDO: melhor rota {meta['best_length']:.1f} em "
                         f"{meta['wall_seconds']:.1f}s ({meta['n_islands']} ilhas, "
                         f"{len(meta['reassignments'])} reatribuição(ões))")

    # ------------------------------------------------------------ trabalhador

    def _dist(self, jid: str, spec: dict) -> np.ndarray:
        d = self._dist_cache.get(jid)
        if d is None:
            if len(self._dist_cache) > 8:
                self._dist_cache.clear()
            d = self._dist_cache[jid] = distance_matrix(spec["coords"])
        return d

    def on_assign(self, req: pb.IslandAssignment, context) -> pb.Ack:
        node = self.node
        lam = node.recv(req.h)
        if req.h.term < node.term:
            return pb.Ack(h=node.hdr(), ok=False, info="mandato antigo")
        key = (req.job_id, req.island_id)
        node.event("assign_recv", msg=f"assign:{req.job_id}:{req.island_id}:{req.h.lamport}", lamport=lam,
                   island=req.island_id, epoch=req.start_epoch)
        with self.lock:
            if key in self.completed:
                self.pool.submit(self._resend_final, key)
                return pb.Ack(h=node.hdr(), ok=True, info="already_done")
            r = self.runners.get(key)
            if r is not None and r.is_alive():
                return pb.Ack(h=node.hdr(), ok=True, info="already_running")
            for k2, r2 in list(self.runners.items()):
                if k2[0] != req.job_id or not r2.is_alive():
                    r2.stop()
                    self.runners.pop(k2, None)
            spec = json.loads(req.spec_json)
            pop = ga.unpack(req.population, len(spec["coords"])) if req.population else None
            runner = IslandRunner(node, req.job_id, req.island_id, spec, req.start_epoch, pop,
                                  self._dist(req.job_id, spec))
            self.runners[key] = runner
            runner.start()
        origin = f"checkpoint da época {req.start_epoch}" if pop is not None else "população nova"
        node.local_log(f"Executando ilha {req.island_id} ({origin}, até a época {spec['epochs']})")
        return pb.Ack(h=node.hdr(), ok=True)

    def _resend_final(self, key) -> None:
        old = self.completed.get(key)
        if old is None:
            return
        req = pb.ProgressReport()
        req.CopyFrom(old)
        req.h.CopyFrom(self.node.hdr())
        self.node.send_to_leader("ReportProgress", req, timeout=3.0)

    def mark_completed(self, jid: str, iid: int, req: pb.ProgressReport) -> None:
        with self.lock:
            self.completed[(jid, iid)] = req
            if len(self.completed) > 256:
                self.completed.pop(next(iter(self.completed)))

    def on_migrate(self, req: pb.Migration, context) -> pb.Ack:
        node = self.node
        lam = node.recv(req.h)
        node.event("migr_recv", msg=req.msg_id, lamport=lam, frm=req.from_island, to=req.to_island,
                   epoch=req.epoch)
        r = self.runners.get((req.job_id, req.to_island))
        if r is None or not r.is_alive():
            return pb.Ack(h=node.hdr(), ok=False, info="island_not_here")
        r.push_immigrants(ga.unpack(req.tours, req.n_cities))
        return pb.Ack(h=node.hdr(), ok=True)

    def on_cancel(self, req: pb.CancelRequest, context) -> pb.Ack:
        node = self.node
        node.recv(req.h)
        with self.lock:
            for (jid, iid), r in list(self.runners.items()):
                if jid == req.job_id and req.island_id in (-1, iid):
                    r.stop()
        return pb.Ack(h=node.hdr(), ok=True)

    def running_islands_for_heartbeat(self, job_id: str) -> list[int]:
        with self.lock:
            alive = [iid for (jid, iid), r in self.runners.items() if jid == job_id and r.is_alive()]
            done = [iid for (jid, iid) in self.completed if jid == job_id]
        return sorted(set(alive) | set(done))

    def running_islands(self) -> list[dict]:
        with self.lock:
            return [{"job": jid, "island": iid, "epoch": r.epoch, "epochs": r.epochs,
                     "best": r.best, "migrations_sent": r.migrations_sent,
                     "migrations_failed": r.migrations_failed, "immigrants": r.immigrants_received}
                    for (jid, iid), r in self.runners.items() if r.is_alive()]

    def stop_all(self) -> None:
        with self.lock:
            for r in self.runners.values():
                r.stop()

    def summary(self) -> dict | None:
        jid = self.current_job_id()
        if not jid:
            hist = self.store.get("jobs/history") or []
            jid = hist[-1] if hist else None
        if not jid:
            return None
        meta = self.meta(jid)
        if not meta:
            return None
        recs = self.islands(jid)
        epochs = max(1, int(meta["epochs"]))
        progress = min((r["epoch"] for r in recs), default=0) / epochs
        return {"id": jid, "label": meta.get("label"), "status": meta["status"], "progress": progress,
                "best_length": meta.get("best_length"), "n_islands": meta["n_islands"],
                "reassignments": len(meta.get("reassignments", [])),
                "wall_seconds": meta.get("wall_seconds"),
                "islands": [{"island": r["island"], "owner": r["owner"], "epoch": r["epoch"],
                             "best": r["best_length"], "finished": r["finished"],
                             "compute_seconds": r["compute_seconds"], "history": r["history"]} for r in recs]}
