"""Sincronização de relógios físicos pelo algoritmo de Berkeley.

O líder atua como mestre (time daemon):
  1. pergunta a hora a todos os nós (GetTime) medindo o RTT de cada consulta;
  2. estima o desvio de cada nó como (hora_remota + RTT/2) - hora_mestre;
  3. calcula a média dos desvios, ignorando leituras discrepantes (outliers)
     e leituras com RTT alto — média tolerante a falhas;
  4. envia a cada nó o AJUSTE (média - desvio_do_nó), em vez da hora
     absoluta, para que o atraso da mensagem de volta não afete o resultado;
  5. aplica o próprio ajuste.

Uma segunda medição (sem ajuste) mede o desvio residual após a rodada.
Rodadas acontecem quando um líder assume e a cada BERKELEY_PERIOD.
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from . import config
from .clocks import ClockSample, berkeley_compute
from .rpc import arquipelago_pb2 as pb

log = logging.getLogger("arquipelago")


class BerkeleySync:
    def __init__(self, node):
        self.node = node
        self.round = 0
        self.pool = ThreadPoolExecutor(max_workers=12, thread_name_prefix="berkeley")
        self._requested = threading.Event()
        self._lock = threading.Lock()
        self.last_result: dict | None = None

    def request_round(self) -> None:
        self._requested.set()

    def sync_newcomer(self, member) -> None:
        """Nó que (re)entra num cluster já sincronizado: só ELE é ajustado.

        Uma rodada completa colocaria o desvio arbitrário do recém-chegado na
        média e deslocaria o relógio de todos os outros nós.
        """
        node = self.node
        if node.store.get("berkeley/last") is None:
            return
        time.sleep(0.3)
        res = self._sample(member, 0)
        if res is None:
            return
        sample, _ = res
        delta = -sample.offset
        rep = node.call(member.address, "AdjustTime", pb.AdjustRequest(h=node.hdr(), round=0, delta=delta),
                        timeout=1.0)
        if rep is not None and rep.ok:
            node.local_log(f"Relógio do nó {member.id} alinhado ao cluster ({delta * 1000:+.1f} ms)")

    def run(self) -> None:
        last = 0.0
        while self.node.running:
            self._requested.wait(1.0)
            if not self.node.is_leader:
                continue
            if not self.node.auto_clock_sync and self.node.store.get("berkeley/last") is None:
                self._requested.clear()  # aguardando a rodada explícita da bateria de testes
                continue
            due = time.monotonic() - last > config.BERKELEY_PERIOD
            if self._requested.is_set() or due:
                self._requested.clear()
                time.sleep(0.5)  # deixa os seguidores receberem o COORDENADOR primeiro
                try:
                    self.run_round(verify=False)
                except Exception:
                    log.exception("erro na rodada de Berkeley")
                last = time.monotonic()

    def _sample(self, m, rnd: int):
        node = self.node
        t0 = time.perf_counter()
        rep = node.call(m.address, "GetTime", pb.TimeRequest(h=node.hdr(), round=rnd), timeout=1.0)
        t1 = time.perf_counter()
        master_now, master_os = node.clock.now(), time.time()
        if rep is None:
            return None
        rtt = t1 - t0
        sample = ClockSample(m.id, rep.time, rtt, master_now)
        os_offset = (rep.os_time + rtt / 2) - master_os
        return sample, os_offset

    def measure(self, rnd: int) -> tuple[list[ClockSample], dict[int, float]]:
        members = self.node.member_list(include_self=False)
        results = [r for r in self.pool.map(lambda m: self._sample(m, rnd), members) if r]
        return [s for s, _ in results], {s.node_id: o for s, o in results}

    def run_round(self, verify: bool = True) -> dict:
        node = self.node
        with self._lock:
            self.round += 1
            rnd = self.round
            samples, os_offsets = self.measure(rnd)
            # Cluster já sincronizado: um nó recém-reiniciado (relógio "cru") é outlier
            # e não pode puxar a média de todos os outros.
            synced = node.store.get("berkeley/last") is not None
            tol = config.BERKELEY_SYNCED_TOLERANCE if synced else config.BERKELEY_OUTLIER_TOLERANCE
            res = berkeley_compute(node.id, samples, tol, config.BERKELEY_MAX_RTT)
            members = {m.id: m for m in node.member_list(include_self=False)}
            applied = {}
            for nid, delta in res["adjustments"].items():
                if nid == node.id:
                    node.clock.adjust(delta)
                    applied[nid] = True
                elif nid in members:
                    rep = node.call(members[nid].address, "AdjustTime",
                                    pb.AdjustRequest(h=node.hdr(), round=rnd, delta=delta), timeout=1.0)
                    applied[nid] = bool(rep and rep.ok)
            lam = node.lamport.tick()
            node.event("berkeley_round", lamport=lam, round=rnd, avg_ms=round(res["average_offset"] * 1000, 3),
                       nodes=len(res["offsets"]))
            result = {
                "round": rnd,
                "master": node.id,
                "t": node.clock.now(),
                "before_ms": {str(k): v * 1000 for k, v in res["offsets"].items()},
                "rtt_ms": {str(k): v * 1000 for k, v in res["rtts"].items()},
                "os_offset_ms": {str(k): v * 1000 for k, v in os_offsets.items()},
                "adjust_ms": {str(k): v * 1000 for k, v in res["adjustments"].items()},
                "average_ms": res["average_offset"] * 1000,
                "used": res["used"],
                "discarded": res["discarded"],
                "applied": {str(k): v for k, v in applied.items()},
            }
            if verify:
                time.sleep(0.2)
                after, _ = self.measure(rnd)
                result["after_ms"] = {str(s.node_id): s.offset * 1000 for s in after}
                result["after_ms"][str(node.id)] = 0.0
            self.last_result = result
            if node.is_leader:
                node.store.put("berkeley/last", result)
            spread = max(res["offsets"].values()) - min(res["offsets"].values())
            node.local_log(f"Berkeley rodada {rnd}: {len(res['offsets'])} relógios, "
                           f"dispersão antes {spread * 1000:.1f} ms, média {res['average_offset'] * 1000:+.1f} ms")
            return result

    # ------------------------------------------------------------ handlers

    def on_get_time(self, req: pb.TimeRequest, context) -> pb.TimeReply:
        node = self.node
        node.recv(req.h)
        return pb.TimeReply(h=node.hdr(), time=node.clock.now(), os_time=time.time())

    def on_adjust(self, req: pb.AdjustRequest, context) -> pb.Ack:
        node = self.node
        lam = node.recv(req.h)
        node.clock.adjust(req.delta)
        node.event("clock_adjust", lamport=lam, round=req.round, delta_ms=round(req.delta * 1000, 3))
        if abs(req.delta) > 0.001:
            node.local_log(f"Relógio ajustado em {req.delta * 1000:+.1f} ms (Berkeley, rodada {req.round})")
        return pb.Ack(h=node.hdr(), ok=True)
