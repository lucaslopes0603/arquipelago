"""Bateria de testes automática (usada pelo executável e pelo modo --local).

Roda sempre no LÍDER atual. O andamento fica no estado replicado (chave
"test"), então, se o líder cair no meio do teste (o que o próprio teste
provoca na fase 5), o novo líder continua da mesma fase.

Fases:
  1 connectivity  RTT gRPC entre o líder e cada nó
  2 berkeley      rodada de Berkeley com medição antes/depois
  3 dist_job      AG distribuído (uma ilha por nó)
  4 baseline_job  mesmo problema e mesma população total em 1 só nó
  5 fault_job     AG com falha de um trabalhador e depois do líder
  6 consistency   compara o hash do estado replicado em todos os nós
  7 report        junta logs de eventos, confere Lamport e gera o relatório
"""

import json
import logging
import os
import statistics
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

from . import tsp
from .events import format_event, merge_and_check, read_all
from .report import build_report
from .rpc import arquipelago_pb2 as pb

log = logging.getLogger("arquipelago")

PHASES = ["connectivity", "berkeley", "dist_job", "baseline_job", "fault_job", "consistency", "report", "done"]

DEFAULT_OPTIONS = {
    "n_cities": 150,
    "instance_seed": 2026,
    "seed": 7,
    "pop_per_island": 800,
    "epochs": 40,
    "gens_per_epoch": 20,
    "fault_pop_per_island": 800,
    "fault_epochs": 100,
    "fault_gens_per_epoch": 20,
    "baseline": True,
    "faults": True,
    "worker_crash_at": 0.2,
    "leader_crash_at": 0.5,
}


class LeadershipLost(Exception):
    pass


class TestOrchestrator:
    def __init__(self, node):
        self.node = node
        self.pool = ThreadPoolExecutor(max_workers=12, thread_name_prefix="orq")
        self._logged_progress: dict[str, int] = {}

    # ------------------------------------------------------------ controle

    def start_test(self, requested_by: str, options: dict) -> tuple[bool, str]:
        node = self.node
        t = node.store.get("test")
        if t and t.get("phase") != "done":
            return False, f"o teste {t['id']} já está em andamento"
        if node.jobs.current_job_id():
            return False, "há um job em execução"
        opts = dict(DEFAULT_OPTIONS, **(options or {}))
        test_id = time.strftime("%Y%m%d-%H%M%S")
        members = node.member_list()
        node.store.put("test", {"id": test_id, "phase": PHASES[0], "started_at": node.clock.now(),
                                "requested_by": requested_by, "options": opts, "results": {}, "fault": {},
                                "jobs": {}, "report": "", "errors": {},
                                "initial_members": [m.to_dict() for m in members]})
        node.cluster_log(f"=== BATERIA DE TESTES {test_id} iniciada com {len(members)} nó(s) "
                         f"(pedido de {requested_by}) ===")
        return True, test_id

    def run(self) -> None:
        node = self.node
        while node.running:
            try:
                if node.is_leader:
                    t = node.store.get("test")
                    if t and t.get("phase") not in (None, "done"):
                        self._step(t)
            except LeadershipLost:
                pass
            except Exception as e:  # uma fase com erro não pode travar o teste
                log.exception("erro na fase do teste")
                t = node.store.get("test") or {}
                phase = t.get("phase")
                if phase and node.is_leader:
                    node.cluster_log(f"Erro na fase {phase}: {e!r}")
                    self._finish_phase(phase, {"error": repr(e)})
            time.sleep(0.3)

    def _step(self, t: dict) -> None:
        phase = t["phase"]
        result = getattr(self, f"_phase_{phase}")(t)
        if result is not None:  # a fase "report" grava o estado final ela mesma
            self._finish_phase(phase, result)

    def _finish_phase(self, phase: str, result) -> None:
        self._check_leader()
        nxt = PHASES[PHASES.index(phase) + 1]

        def upd(t):
            t = dict(t)
            t["results"] = dict(t["results"], **{phase: result})
            t["phase"] = nxt
            return t
        self.node.store.update("test", upd)

    def _patch(self, field: str, **values) -> dict:
        def upd(t):
            t = dict(t)
            t[field] = dict(t.get(field) or {}, **values)
            return t
        return self.node.store.update("test", upd)

    def _check_leader(self) -> None:
        if not self.node.is_leader or not self.node.running:
            raise LeadershipLost()

    def _sleep(self, seconds: float) -> None:
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            self._check_leader()
            time.sleep(min(0.2, max(0.0, end - time.monotonic())))

    # ------------------------------------------------------------ fase 1

    def _phase_connectivity(self, t: dict) -> dict:
        node = self.node
        node.cluster_log("[1/7] Conectividade: medindo RTT gRPC até cada nó (5 pings)")

        def probe(m):
            rtts = []
            for _ in range(5):
                t0 = time.perf_counter()
                rep = node.call(m.address, "Ping", pb.PingRequest(h=node.hdr()), timeout=1.0)
                if rep is not None:
                    rtts.append((time.perf_counter() - t0) * 1000)
            return {"id": m.id, "host": m.host, "address": m.address, "sent": 5, "received": len(rtts),
                    "rtt_avg_ms": statistics.mean(rtts) if rtts else None,
                    "rtt_min_ms": min(rtts) if rtts else None, "rtt_max_ms": max(rtts) if rtts else None}

        rows = list(self.pool.map(probe, node.member_list(include_self=False)))
        for r in rows:
            rtt = f"{r['rtt_avg_ms']:.2f} ms" if r["rtt_avg_ms"] is not None else "SEM RESPOSTA"
            node.cluster_log(f"      nó {r['id']} ({r['host']}, {r['address']}): {r['received']}/5 respostas, "
                             f"RTT médio {rtt}")
        return {"leader": {"id": node.id, "host": node.hostname, "address": node.address},
                "term": node.term, "nodes": rows, "n_nodes": len(rows) + 1,
                "history": node.store.get("history") or []}

    # ------------------------------------------------------------ fase 2

    def _phase_berkeley(self, t: dict) -> dict:
        node = self.node
        node.cluster_log("[2/7] Sincronização de relógios físicos (Berkeley)")
        res = node.berkeley.run_round(verify=True)
        before = list(res["before_ms"].values())
        after = list(res.get("after_ms", {}).values()) or [0.0]
        res["spread_before_ms"] = max(before) - min(before)
        res["spread_after_ms"] = max(after) - min(after)
        for nid in sorted(res["before_ms"], key=int):
            node.cluster_log(f"      nó {nid}: desvio {res['before_ms'][nid]:+9.1f} ms, ajuste "
                             f"{res['adjust_ms'][nid]:+9.1f} ms, depois {res.get('after_ms', {}).get(nid, 0):+7.2f} ms")
        node.cluster_log(f"      dispersão entre relógios: {res['spread_before_ms']:.1f} ms -> "
                         f"{res['spread_after_ms']:.2f} ms")
        return res

    # ------------------------------------------------------------ jobs

    def _coords(self, t: dict):
        o = t["options"]
        return tsp.random_instance(int(o["n_cities"]), int(o["instance_seed"]))

    def _run_job(self, t: dict, slot: str, make_spec, title: str, monitor=None) -> dict:
        node = self.node
        jid = (t.get("jobs") or {}).get(slot)
        if not jid:
            current = node.jobs.current_job_id()
            if current and current.startswith(slot):
                jid = current  # job já criado por um líder anterior
            else:
                ok, info = node.jobs.submit(make_spec())
                if not ok:
                    raise RuntimeError(info)
                jid = info
            self._patch("jobs", **{slot: jid})
        last_quarter = self._logged_progress.get(jid, 0)
        while True:
            self._check_leader()
            meta = node.jobs.meta(jid)
            if meta is None:
                raise RuntimeError(f"job {jid} não encontrado no estado replicado")
            if meta["status"] == "done":
                return meta
            recs = node.jobs.islands(jid)
            epochs = max(1, int(meta["epochs"]))
            progress = sum(r["epoch"] for r in recs) / (epochs * max(1, len(recs)))
            quarter = int(progress * 4)
            if quarter > last_quarter and quarter < 4:
                last_quarter = self._logged_progress[jid] = quarter
                best = meta.get("best_length")
                node.cluster_log(f"      {title}: {quarter * 25}% concluído"
                                 + (f", melhor rota até agora {best:.1f}" if best else ""))
            if monitor:
                monitor(jid, meta, recs, progress)
            time.sleep(0.4)

    @staticmethod
    def _job_result(meta: dict) -> dict:
        return {"job_id": meta["id"], "wall_seconds": meta["wall_seconds"], "best_length": meta["best_length"],
                "best_tour": meta["best_tour"], "n_islands": meta["n_islands"], "pop_size": meta["pop_size"],
                "epochs": meta["epochs"], "gens_per_epoch": meta["gens_per_epoch"], "nodes": meta["nodes"],
                "reassignments": meta["reassignments"], "islands": meta.get("islands_summary", []),
                "leader_at_start": meta.get("leader_at_start"), "leader_at_end": meta.get("leader_at_end")}

    def _phase_dist_job(self, t: dict) -> dict:
        node = self.node
        o = t["options"]
        k = len(node.member_ids())
        node.cluster_log(f"[3/7] AG distribuído: {k} ilha(s), uma por nó, migração em anel a cada época")

        def spec():
            return {"label": "dist_job", "coords": self._coords(t), "islands": k,
                    "pop_size": int(o["pop_per_island"]) * k, "epochs": int(o["epochs"]),
                    "gens_per_epoch": int(o["gens_per_epoch"]), "seed": int(o["seed"])}
        return self._job_result(self._run_job(t, "dist_job", spec, "AG distribuído"))

    def _phase_baseline_job(self, t: dict) -> dict:
        node = self.node
        o = t["options"]
        dist = t["results"].get("dist_job") or {}
        if not o.get("baseline") or "pop_size" not in dist:
            return {"skipped": "linha de base desativada ou job distribuído sem resultado"}
        node.cluster_log(f"[4/7] Linha de base: mesmo problema e população total ({dist['pop_size']}) "
                         f"em UMA máquina (nó {node.id})")

        def spec():
            return {"label": "baseline_job", "coords": self._coords(t), "islands": 1, "pin_to": node.id,
                    "pop_size": dist["pop_size"], "epochs": dist["epochs"],
                    "gens_per_epoch": dist["gens_per_epoch"], "seed": int(o["seed"])}
        res = self._job_result(self._run_job(t, "baseline_job", spec, "Linha de base"))
        res["speedup"] = res["wall_seconds"] / dist["wall_seconds"] if dist.get("wall_seconds") else None
        res["efficiency"] = res["speedup"] / dist["n_islands"] if res["speedup"] else None
        node.cluster_log(f"      1 máquina: {res['wall_seconds']:.1f}s | {dist['n_islands']} máquinas: "
                         f"{dist['wall_seconds']:.1f}s | speedup {res['speedup']:.2f}x")
        return res

    def _phase_fault_job(self, t: dict) -> dict:
        node = self.node
        o = t["options"]
        if not o.get("faults"):
            return {"skipped": "injeção de falhas desativada"}
        k = len(node.member_ids())
        if not (t.get("jobs") or {}).get("fault_job"):
            node.cluster_log(f"[5/7] Tolerância a falhas: AG com {k} ilha(s); um trabalhador e depois o líder "
                             "serão derrubados durante a execução")

        # Com 2 nós, depois da queda do trabalhador o líder fica sozinho e só pode ser
        # derrubado quando o outro voltar (reinício leva ~10 s): o job precisa durar mais.
        epochs = int(o["fault_epochs"]) * (16 if k <= 2 else 10) // 10

        def spec():
            return {"label": "fault_job", "coords": self._coords(t), "islands": k,
                    "pop_size": int(o["fault_pop_per_island"]) * k, "epochs": epochs,
                    "gens_per_epoch": int(o["fault_gens_per_epoch"]), "seed": int(o["seed"]) + 1}
        meta = self._run_job(t, "fault_job", spec, "AG com falhas", monitor=self._fault_monitor)
        t = node.store.get("test")
        return self._fault_result(t, meta)

    def _fault_monitor(self, jid, meta, recs, progress) -> None:
        node = self.node
        t = node.store.get("test")
        o, f = t["options"], t.get("fault") or {}
        others = node.member_list(include_self=False)
        now = node.clock.now()
        if "worker_crash" not in f and "worker_skip" not in f:
            if progress < float(o["worker_crash_at"]):
                return
            if not others:
                self._patch("fault", worker_skip="só havia um nó no cluster")
                return
            load = Counter(r["owner"] for r in recs if not r["finished"])
            victim = max(others, key=lambda m: (load[m.id], -m.id))
            self._patch("fault", worker_crash={"node": victim.id, "host": victim.host, "t": now,
                                               "progress": progress})
            node.cluster_log(f"      >>> INJETANDO FALHA no trabalhador {victim.id} ({victim.host}): "
                             f"o processo será encerrado ({progress:.0%} do job)")
            node.replicator.wait_synced(3.0)
            lam = node.lamport.tick()
            node.event("inject_worker_crash", lamport=lam, victim=victim.id)
            node.call(victim.address, "InjectCrash", pb.CrashRequest(h=node.hdr(), reason="teste: falha de trabalhador"),
                      timeout=2.0)
            return
        if "leader_crash" in f or "leader_skip" in f:
            return
        wc = f.get("worker_crash")
        history = node.store.get("history") or []
        handled = wc is None or any(h["kind"] in ("dead", "join") and h.get("node") == wc["node"]
                                    and h["t"] >= wc["t"] for h in history)
        if progress >= float(o["leader_crash_at"]) and handled and others:
            self._patch("fault", leader_crash={"node": node.id, "host": node.hostname, "t": node.clock.now(),
                                               "term": node.term, "progress": progress})
            node.cluster_log(f"      >>> INJETANDO FALHA NO LÍDER (nó {node.id}, {node.hostname}) com "
                             f"{progress:.0%} do job: os demais devem eleger outro líder e continuar")
            node.replicator.wait_synced(5.0)
            time.sleep(0.3)
            lam = node.lamport.tick()
            node.event("inject_leader_crash", lamport=lam)
            node.crash("teste: falha do líder")
            raise LeadershipLost()
        if progress >= 0.9:
            reason = "não havia outro nó vivo" if not others else "a falha do trabalhador ainda não tinha sido tratada"
            self._patch("fault", leader_skip=reason)

    def _fault_result(self, t: dict, meta: dict) -> dict:
        f = t.get("fault") or {}
        history = self.node.store.get("history") or []
        res = {"job": self._job_result(meta), "fault": f}
        wc = f.get("worker_crash")
        if wc:
            after = [h for h in history if h.get("node") == wc["node"] and h["t"] >= wc["t"]]
            dead = next((h for h in after if h["kind"] == "dead"), None)
            back = next((h for h in after if h["kind"] == "join"), None)
            moved = [r for r in meta["reassignments"] if r["from"] == wc["node"]]
            res["worker"] = {
                "node": wc["node"], "host": wc.get("host"),
                "detected_after_s": dead["t"] - wc["t"] if dead else None,
                "reassigned": moved,
                "reassigned_after_s": moved[0]["t"] - wc["t"] if moved else None,
                "rejoined_after_s": back["t"] - wc["t"] if back else None}
        lc = f.get("leader_crash")
        if lc:
            after = [h for h in history if h["t"] >= lc["t"]]
            new = next((h for h in after if h["kind"] == "leader" and h.get("node") != lc["node"]), None)
            back = next((h for h in after if h["kind"] == "join" and h.get("node") == lc["node"]), None)
            moved = [r for r in meta["reassignments"] if r["from"] == lc["node"] and r["t"] >= lc["t"]]
            res["leader"] = {
                "old": lc["node"], "host": lc.get("host"), "old_term": lc.get("term"),
                "new": new["node"] if new else None, "new_term": new.get("term") if new else None,
                "failover_s": new["t"] - lc["t"] if new else None,
                "reassigned": moved, "rejoined_after_s": back["t"] - lc["t"] if back else None,
                "leaders_after": [{"node": h["node"], "term": h.get("term"), "dt": h["t"] - lc["t"],
                                   "reason": h.get("reason")} for h in after if h["kind"] == "leader"]}
        return res

    # ------------------------------------------------------------ fase 6

    def _phase_consistency(self, t: dict) -> dict:
        node = self.node
        node.cluster_log("[6/7] Consistência das réplicas: comparando o hash do estado em todos os nós")
        time.sleep(1.0)
        rows, leader_digest, version = [], None, None
        for _ in range(6):
            self._check_leader()
            node.replicator.wait_synced(5.0)
            leader_digest, version = node.store.digest(), node.store.version

            def fetch(m):
                rep = node.call(m.address, "GetStatus", pb.Empty(), timeout=5.0)
                if rep is None:
                    return {"id": m.id, "host": m.host, "reachable": False}
                st = json.loads(rep.json)["store"]
                return {"id": m.id, "host": m.host, "reachable": True, "version": st["version"],
                        "term": st["term"], "digest": st["digest"]}
            rows = list(self.pool.map(fetch, node.member_list(include_self=False)))
            if all(r.get("digest") == leader_digest for r in rows):
                break
            time.sleep(0.5)
        all_match = all(r.get("digest") == leader_digest for r in rows)
        result = {"leader": node.id, "leader_digest": leader_digest, "version": version, "term": node.store.term,
                  "replicas": rows, "all_match": all_match,
                  "full_syncs": node.replicator.full_syncs, "deltas": node.replicator.deltas}
        node.cluster_log(f"      versão {version}, hash {leader_digest}: "
                         + ("todas as réplicas idênticas" if all_match else "DIVERGÊNCIA encontrada"))
        return result

    # ------------------------------------------------------------ fase 7

    def _phase_report(self, t: dict) -> None:
        node = self.node
        node.cluster_log("[7/7] Coletando eventos de todos os nós para conferir os relógios de Lamport")

        def fetch(m):
            rep = node.call(m.address, "GetEvents", pb.Empty(), timeout=10.0)
            return json.loads(rep.json) if rep is not None else []
        lists = [read_all(node.data_dir, node.id)] + list(self.pool.map(fetch, node.member_list(include_self=False)))
        check = merge_and_check(lists)
        path = os.path.join(node.data_dir, f"linha_do_tempo_lamport_{t['id']}.txt")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("Linha do tempo global ordenada por (relógio de Lamport, id do nó)\n\n")
            for e in check["events"]:
                fh.write(format_event(e) + "\n")
            fh.write(f"\nPares envio/recebimento: {check['pairs']} | violações: {len(check['violations'])}\n")
        sample, per_kind = [], Counter()
        for s, r in check["cross_pairs"]:
            kind = s["kind"].split("_")[0]
            if per_kind[kind] >= (4 if kind == "migr" else 2):
                continue
            per_kind[kind] += 1
            sample.append(format_event(s))
            sample.append("   └─> " + format_event(r))
        lam = {"events": len(check["events"]), "pairs": check["pairs"],
               "cross_node_pairs": check["cross_node_pairs"], "violations": len(check["violations"]),
               "nodes": len(lists), "file": path, "file_host": node.hostname, "sample": sample}
        node.cluster_log(f"      {lam['events']} eventos, {lam['pairs']} pares envio->recebimento "
                         f"({lam['cross_node_pairs']} entre nós diferentes), {lam['violations']} violações")

        t = node.store.get("test")
        results = dict(t["results"], report=lam)
        finished = dict(t, results=results, finished_at=node.clock.now(), final_leader=node.id)
        text, checks = build_report(finished, node.member_list())
        finished["report"] = text
        finished["checks"] = checks
        finished["phase"] = "done"
        node.store.put("test", finished)
        ok = sum(1 for c in checks if c["status"] == "OK")
        node.cluster_log(f"=== TESTE {t['id']} CONCLUÍDO: {ok}/{len(checks)} verificações OK ===")
        node.replicator.wait_synced(5.0)
        return None
