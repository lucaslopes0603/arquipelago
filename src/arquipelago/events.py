"""Log de eventos de cada nó, carimbado com Lamport e com o relógio sincronizado.

Eventos de envio/recebimento de uma mesma mensagem compartilham o campo `msg`
(ex.: "migr:<uuid>"). Com isso, ao juntar os logs de todos os nós, é possível
verificar a condição de relógio de Lamport: L(envio) < L(recebimento).
"""

import glob
import json
import os
import threading
from collections import deque
from datetime import datetime


class EventLog:
    def __init__(self, node_id: int, path: str | None = None, maxlen: int = 50000):
        self.node_id = node_id
        self.path = path
        self._events = deque(maxlen=maxlen)
        self._lock = threading.Lock()
        self._file = open(path, "a", encoding="utf-8") if path else None

    def record(self, lamport: int, phys_time: float, kind: str, msg: str = "", **data) -> None:
        ev = {"lamport": int(lamport), "node": self.node_id, "t": round(phys_time, 6),
              "kind": kind, "msg": msg, "data": data}
        with self._lock:
            self._events.append(ev)
            if self._file:
                self._file.write(json.dumps(ev, ensure_ascii=False) + "\n")
                self._file.flush()

    def all(self) -> list[dict]:
        with self._lock:
            return list(self._events)


def _files(data_dir: str, node_id: int) -> list[str]:
    return sorted(glob.glob(os.path.join(data_dir, f"eventos_no{node_id}_inc*.jsonl")))


def clear_previous(data_dir: str, node_id: int) -> None:
    """Início "do zero" (encarnação 0): apaga eventos de execuções antigas com o mesmo id."""
    for path in _files(data_dir, node_id):
        try:
            os.remove(path)
        except OSError:
            pass


def read_all(data_dir: str, node_id: int) -> list[dict]:
    """Eventos de todas as encarnações deste nó (armazenamento estável em disco)."""
    events = []
    for path in _files(data_dir, node_id):
        try:
            with open(path, encoding="utf-8") as fh:
                for line in fh:
                    try:
                        events.append(json.loads(line))
                    except ValueError:
                        pass  # linha cortada pelo crash
        except OSError:
            pass
    return events


def max_lamport(data_dir: str, node_id: int) -> int:
    return max((e["lamport"] for e in read_all(data_dir, node_id)), default=0)


def merge_and_check(event_lists: list[list[dict]]) -> dict:
    """Junta logs de vários nós em ordem total (lamport, nó) e confere a causalidade."""
    merged = sorted((e for evs in event_lists for e in evs), key=lambda e: (e["lamport"], e["node"]))
    sends, recvs = {}, {}
    for e in merged:
        if not e.get("msg"):
            continue
        if e["kind"].endswith("_send"):
            sends.setdefault(e["msg"], e)
        elif e["kind"].endswith("_recv"):
            recvs.setdefault(e["msg"], e)
    pairs = [(sends[m], recvs[m]) for m in recvs if m in sends]
    violations = [(s, r) for s, r in pairs if not s["lamport"] < r["lamport"]]
    cross = sorted(((s, r) for s, r in pairs if s["node"] != r["node"]), key=lambda p: p[0]["lamport"])
    return {"events": merged, "pairs": len(pairs), "cross_node_pairs": len(cross), "cross_pairs": cross,
            "violations": violations}


def format_event(e: dict) -> str:
    t = datetime.fromtimestamp(e["t"]).strftime("%H:%M:%S.%f")[:-3]
    extra = " ".join(f"{k}={v}" for k, v in e.get("data", {}).items())
    return f"L={e['lamport']:>6} | nó {e['node']:>6} | {t} | {e['kind']:<16} {extra}"
