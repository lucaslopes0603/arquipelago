"""Teste de integração: cluster real com 3 nós gRPC no mesmo processo.

As falhas aqui usam crash_mode="stop" (servidor derrubado sem aviso), porque
os._exit() mataria o próprio pytest. O executável usa a falha por processo.
"""

import time

import pytest

from arquipelago import tsp
from arquipelago.node import Node

BASE = 51210


def wait(cond, timeout=20.0, step=0.1):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return True
        time.sleep(step)
    return False


@pytest.fixture
def cluster(tmp_path):
    ids = [3, 7, 9]
    ports = {i: BASE + k for k, i in enumerate(ids)}
    peers = [f"127.0.0.1:{p}" for p in ports.values()]
    nodes = {i: Node(i, port=ports[i], advertise="127.0.0.1", peers=peers, data_dir=str(tmp_path / f"n{i}"),
                     crash_mode="stop", skew=0.5 * k, auto_clock_sync=False) for k, i in enumerate(ids)}
    for n in nodes.values():
        n.start()
    yield nodes
    for n in nodes.values():
        if n.running:
            n.stop()


def leaders(nodes):
    return [n.id for n in nodes.values() if n.running and n.is_leader]


def test_eleicao_falha_do_lider_e_job(cluster):
    n = cluster
    # Bully: o maior id vence e todos concordam.
    assert wait(lambda: leaders(n) == [9] and all(x.leader_id == 9 for x in n.values()))
    assert wait(lambda: len(n[9].member_ids()) == 3)

    # Job distribuído com uma ilha por nó.
    spec = {"label": "t", "coords": tsp.random_instance(40, 1), "pop_size": 300, "epochs": 40,
            "gens_per_epoch": 10, "seed": 1, "local_search": True}
    ok, jid = n[9].jobs.submit(spec)
    assert ok
    assert wait(lambda: (n[9].jobs.meta(jid) or {}).get("epochs") and
                min(r["epoch"] for r in n[9].jobs.islands(jid)) >= 5, timeout=30)

    # O líder cai: o nó 7 (maior id restante) assume e termina o job a partir da réplica.
    n[9].crash("teste")
    assert wait(lambda: leaders(n) == [7] and n[3].leader_id == 7, timeout=20)
    assert wait(lambda: (n[7].jobs.meta(jid) or {}).get("status") == "done", timeout=60)
    meta = n[7].jobs.meta(jid)
    assert any(r["from"] == 9 for r in meta["reassignments"])
    assert tsp.is_valid_tour(meta["best_tour"], 40)

    # Réplicas consistentes depois de tudo.
    assert wait(lambda: n[3].store.digest() == n[7].store.digest(), timeout=10)


def test_berkeley_sincroniza(cluster):
    n = cluster
    assert wait(lambda: leaders(n) == [9])
    assert wait(lambda: len(n[9].member_ids()) == 3)
    res = n[9].berkeley.run_round(verify=True)
    before = list(res["before_ms"].values())
    after = list(res["after_ms"].values())
    assert max(before) - min(before) > 500         # desvios artificiais de 0, 0,5 e 1 s
    assert max(after) - min(after) < 50            # depois: poucos milissegundos
