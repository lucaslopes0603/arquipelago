"""Testes de unidade dos algoritmos (sem rede)."""

import numpy as np
import pytest

from arquipelago import ga, tsp
from arquipelago.clocks import ClockSample, LamportClock, berkeley_compute
from arquipelago.events import merge_and_check
from arquipelago.store import ReplicatedStore


# ---------------------------------------------------------------- Lamport

def test_lamport_regras_basicas():
    a, b = LamportClock(), LamportClock()
    t_envio = a.tick()               # a envia
    assert t_envio == 1
    t_receb = b.receive(t_envio)     # b recebe: max(0, 1) + 1
    assert t_receb == 2
    b.tick()
    b.tick()
    assert a.receive(b.tick()) == 6  # max(1, 5) + 1


def test_lamport_recebimento_de_carimbo_menor_ainda_avanca():
    c = LamportClock(start=10)
    assert c.receive(3) == 11


# ---------------------------------------------------------------- Berkeley

def test_berkeley_media_e_ajustes():
    # Mestre (id 1) em 0; nó 2 adiantado 100 ms; nó 3 atrasado 40 ms (RTT desprezível).
    samples = [ClockSample(2, 1000.100, 0.0, 1000.0), ClockSample(3, 999.960, 0.0, 1000.0)]
    res = berkeley_compute(1, samples, tolerance=10, max_rtt=0.5)
    assert res["average_offset"] == pytest.approx(0.020)
    adj = res["adjustments"]
    assert adj[1] == pytest.approx(0.020)
    assert adj[2] == pytest.approx(-0.080)
    assert adj[3] == pytest.approx(0.060)
    # Depois do ajuste todos ficam no mesmo instante (média).
    for nid, off in res["offsets"].items():
        assert off + adj[nid] == pytest.approx(0.020)


def test_berkeley_compensa_metade_do_rtt():
    s = ClockSample(2, remote_time=100.0, rtt=0.2, master_time=100.1)
    assert s.offset == pytest.approx(0.0)


def test_berkeley_descarta_outlier():
    samples = [ClockSample(2, 1000.01, 0, 1000), ClockSample(3, 999.99, 0, 1000),
               ClockSample(4, 1600.0, 0, 1000)]  # relógio absurdo
    res = berkeley_compute(1, samples, tolerance=10, max_rtt=0.5)
    assert res["discarded"] == [4]
    assert abs(res["average_offset"]) < 0.01
    assert res["adjustments"][4] == pytest.approx(res["average_offset"] - 600.0)


# ---------------------------------------------------------------- replicação

def test_store_delta_e_snapshot():
    lider, seguidor = ReplicatedStore(), ReplicatedStore()
    lider.start_term(1)
    lider.put("a", 1)
    lider.put("b", {"x": [1, 2]})
    data, term, ver = lider.encode_since(0)
    assert seguidor.apply(data, full=True, base_version=0) == (True, 2)
    lider.put("a", 5)
    lider.delete("b")
    data, _, ver = lider.encode_since(2)
    assert seguidor.apply(data, full=False, base_version=2) == (True, 4)
    assert seguidor.get("a") == 5 and seguidor.get("b") is None
    assert seguidor.digest() == lider.digest()


def test_store_recusa_delta_fora_de_ordem():
    lider, seguidor = ReplicatedStore(), ReplicatedStore()
    lider.start_term(1)
    for i in range(5):
        lider.put(f"k{i}", i)
    data, _, _ = lider.encode_since(3)
    ok, ver = seguidor.apply(data, full=False, base_version=3)  # seguidor está na versão 0
    assert not ok and ver == 0


def test_store_novo_mandato_exige_snapshot():
    lider, seguidor = ReplicatedStore(), ReplicatedStore()
    lider.start_term(1)
    lider.put("a", 1)
    seguidor.apply(lider.encode_since(0)[0], True, 0)
    lider.start_term(2)
    lider.put("a", 2)
    ok, _ = seguidor.apply(lider.encode_since(1)[0], False, 1)
    assert not ok  # mandato mudou: só aceita snapshot completo


# ---------------------------------------------------------------- AG

def test_ox_gera_permutacao():
    rng = np.random.default_rng(0)
    n = 30
    used = np.zeros(n, dtype=bool)
    for _ in range(200):
        p1, p2 = rng.permutation(n).astype(np.int16), rng.permutation(n).astype(np.int16)
        a, b = sorted(rng.integers(0, n + 1, 2))
        child = np.empty(n, dtype=np.int16)
        ga.order_crossover(p1, p2, a, b, child, used)
        assert tsp.is_valid_tour(child, n)
        assert np.array_equal(child[a:b], p1[a:b])


def test_evolucao_mantem_rotas_validas_e_melhora():
    coords = tsp.random_instance(40, 1)
    dist = tsp.distance_matrix(coords)
    rng = np.random.default_rng(1)
    pop = ga.random_population(40, 60, rng)
    antes = ga.tour_lengths(pop, dist).min()
    pop, lengths = ga.evolve(pop, dist, 50, rng)
    assert all(tsp.is_valid_tour(t, 40) for t in pop)
    assert lengths.min() < antes


def test_two_opt_nao_piora():
    coords = tsp.random_instance(50, 3)
    dist = tsp.distance_matrix(coords)
    t = np.random.default_rng(3).permutation(50).astype(np.int16)
    t2 = ga.two_opt(t, dist)
    assert tsp.is_valid_tour(t2, 50)
    assert tsp.tour_length(t2, dist) <= tsp.tour_length(t, dist)


def test_ag_acha_otimo_no_circulo():
    n = 40
    spec = ga.make_spec(tsp.circle_instance(n), pop_size=200, epochs=10, gens_per_epoch=20, islands=1)
    res = ga.run_sequential(spec)
    assert res["best_length"] == pytest.approx(tsp.circle_optimum(n), rel=1e-6)


def test_pack_unpack():
    pop = ga.random_population(25, 7, np.random.default_rng(5))
    assert np.array_equal(ga.unpack(ga.pack(pop), 25), pop)


def test_ilha_deterministica_por_epoca():
    spec = ga.make_spec(tsp.random_instance(30, 2), pop_size=80, gens_per_epoch=5, islands=1)
    a, b = ga.IslandGA(spec, 0), ga.IslandGA(spec, 0)
    a.run_epoch(0)
    b.run_epoch(0)
    assert np.array_equal(a.pop, b.pop)
    # retomar de um checkpoint reproduz a mesma evolução
    c = ga.IslandGA(spec, 0, population=a.pop.copy())
    a.run_epoch(1)
    c.run_epoch(1)
    assert np.array_equal(a.pop, c.pop)


# ---------------------------------------------------------------- Lamport entre nós

def test_merge_detecta_violacao_causal():
    ok = [[{"lamport": 3, "node": 1, "t": 0, "kind": "migr_send", "msg": "m1", "data": {}}],
          [{"lamport": 4, "node": 2, "t": 0, "kind": "migr_recv", "msg": "m1", "data": {}}]]
    assert merge_and_check(ok)["violations"] == []
    ruim = [[{"lamport": 5, "node": 1, "t": 0, "kind": "migr_send", "msg": "m1", "data": {}}],
            [{"lamport": 4, "node": 2, "t": 0, "kind": "migr_recv", "msg": "m1", "data": {}}]]
    assert len(merge_and_check(ruim)["violations"]) == 1
