"""Relógios: lógico (Lamport) e físico sincronizado (algoritmo de Berkeley).

LamportClock
    Regras clássicas: incrementa em cada evento local e envio; ao receber uma
    mensagem com carimbo t, faz relógio = max(relógio, t) + 1. Garante que, se
    a -> b (aconteceu-antes), então L(a) < L(b).

SyncedClock
    Relógio físico "virtual" do nó: hora do SO + desvio artificial (para a
    demonstração ficar visível) + correção acumulada pelo Berkeley. Não mexemos
    no relógio do Windows porque isso exige privilégio de administrador.

berkeley_compute
    Parte matemática do Berkeley, isolada para ser testável: recebe as
    leituras (com RTT) dos nós, descarta outliers e devolve o ajuste de cada um.
"""

import statistics
import threading
import time
from dataclasses import dataclass


class LamportClock:
    def __init__(self, start: int = 0):
        self._value = int(start)
        self._lock = threading.Lock()

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    def tick(self) -> int:
        """Evento local ou envio de mensagem."""
        with self._lock:
            self._value += 1
            return self._value

    def receive(self, remote: int) -> int:
        """Recebimento de mensagem carimbada com `remote`."""
        with self._lock:
            self._value = max(self._value, int(remote)) + 1
            return self._value


class SyncedClock:
    def __init__(self, skew: float = 0.0):
        self.skew = float(skew)   # desvio artificial, fixo durante a vida do processo
        self.offset = 0.0         # correção acumulada pelas rodadas de Berkeley
        self._lock = threading.Lock()

    def now(self) -> float:
        with self._lock:
            return time.time() + self.skew + self.offset

    def adjust(self, delta: float) -> None:
        with self._lock:
            self.offset += float(delta)


@dataclass
class ClockSample:
    node_id: int
    remote_time: float   # hora informada pelo nó
    rtt: float           # tempo de ida e volta da consulta
    master_time: float   # hora do mestre no instante em que a resposta chegou

    @property
    def offset(self) -> float:
        """Quanto o nó está adiantado (+) ou atrasado (-) em relação ao mestre.

        Estimativa de Cristian: a resposta foi gerada ~RTT/2 antes de chegar.
        """
        return (self.remote_time + self.rtt / 2.0) - self.master_time


def berkeley_compute(master_id: int, samples: list[ClockSample],
                     tolerance: float, max_rtt: float) -> dict:
    """Calcula a média tolerante a falhas e o ajuste de cada nó.

    O mestre entra com offset 0. Leituras com RTT alto ou longe da mediana são
    descartadas da média (mas o nó ainda recebe o ajuste, para convergir).
    """
    offsets = {master_id: 0.0}
    rtts = {master_id: 0.0}
    for s in samples:
        offsets[s.node_id] = s.offset
        rtts[s.node_id] = s.rtt
    med = statistics.median(offsets.values())
    used, discarded = [], []
    for nid, off in offsets.items():
        if rtts[nid] > max_rtt or abs(off - med) > tolerance:
            discarded.append(nid)
        else:
            used.append(nid)
    if not used:
        used = [master_id]
    avg = sum(offsets[n] for n in used) / len(used)
    adjustments = {nid: avg - off for nid, off in offsets.items()}
    return {
        "average_offset": avg,
        "offsets": offsets,
        "rtts": rtts,
        "used": sorted(used),
        "discarded": sorted(discarded),
        "adjustments": adjustments,
    }
