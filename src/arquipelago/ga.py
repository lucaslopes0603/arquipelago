"""Algoritmo genético para o TSP executado dentro de uma ilha.

Representação: cada indivíduo é uma permutação das cidades (int16).
Operadores: seleção por torneio, Order Crossover (OX), mutação por inversão,
elitismo e, opcionalmente, busca local 2-opt no melhor indivíduo de cada época
(algoritmo memético).

Este módulo não sabe nada de rede: a distribuição (ilhas, migração,
checkpoints) fica em island.py e jobs.py.
"""

import time

import numpy as np

from .tsp import distance_matrix

POP_DTYPE = np.int16

DEFAULT_SPEC = {
    "label": "job",
    "pop_size": 1200,       # população TOTAL, dividida entre as ilhas
    "islands": 0,           # 0 = uma ilha por nó vivo
    "epochs": 30,           # migração e checkpoint a cada época
    "gens_per_epoch": 20,
    "migrants": 4,
    "crossover_rate": 0.9,
    "mutation_rate": 0.3,
    "tournament": 3,
    "elite": 2,
    "local_search": True,
    "seed": 42,
    "pin_to": 0,            # != 0 força todas as ilhas em um único nó (linha de base)
}


def make_spec(coords, **overrides) -> dict:
    spec = dict(DEFAULT_SPEC)
    spec.update(overrides)
    spec["coords"] = coords
    return spec


def island_pop_size(spec: dict) -> int:
    return max(10, int(spec["pop_size"]) // max(1, int(spec["islands"])))


# ---------------------------------------------------------------- serialização

def pack(pop: np.ndarray) -> bytes:
    return np.ascontiguousarray(pop, dtype="<i2").tobytes()


def unpack(data: bytes, n_cities: int) -> np.ndarray:
    return np.frombuffer(data, dtype="<i2").reshape(-1, n_cities).astype(POP_DTYPE)


# ---------------------------------------------------------------- operadores

def tour_lengths(pop: np.ndarray, dist: np.ndarray) -> np.ndarray:
    idx = pop.astype(np.intp)
    return dist[idx, np.roll(idx, -1, axis=1)].sum(axis=1)


def random_population(n_cities: int, size: int, rng: np.random.Generator) -> np.ndarray:
    return np.argsort(rng.random((size, n_cities)), axis=1).astype(POP_DTYPE)


def order_crossover(p1, p2, a: int, b: int, out: np.ndarray, used: np.ndarray) -> None:
    """OX: copia p1[a:b] e completa com as cidades restantes na ordem em que aparecem em p2."""
    used[:] = False
    seg = p1[a:b]
    used[seg] = True
    rest = p2[~used[p2]]
    out[a:b] = seg
    out[:a] = rest[:a]
    out[b:] = rest[a:]


def two_opt(tour: np.ndarray, dist: np.ndarray, max_passes: int = 30) -> np.ndarray:
    """Busca local 2-opt (primeira melhora por posição i, vetorizada em j)."""
    t = tour.astype(np.intp).copy()
    n = len(t)
    for _ in range(max_passes):
        improved = False
        for i in range(n - 2):
            a, b = t[i], t[i + 1]
            j = np.arange(i + 2, n if i > 0 else n - 1)
            if len(j) == 0:
                continue
            c = t[j]
            d = t[(j + 1) % n]
            delta = dist[a, c] + dist[b, d] - dist[a, b] - dist[c, d]
            k = int(np.argmin(delta))
            if delta[k] < -1e-9:
                jj = int(j[k])
                t[i + 1:jj + 1] = t[i + 1:jj + 1][::-1]
                improved = True
        if not improved:
            break
    return t.astype(POP_DTYPE)


def evolve(pop: np.ndarray, dist: np.ndarray, generations: int, rng: np.random.Generator,
           crossover_rate=0.9, mutation_rate=0.3, tournament=3, elite=2):
    """Executa `generations` gerações e devolve (população, comprimentos)."""
    size, n = pop.shape
    elite = min(elite, size - 1)
    m = size - elite
    used = np.zeros(n, dtype=bool)
    for _ in range(generations):
        lengths = tour_lengths(pop, dist)
        order = np.argsort(lengths)
        new = np.empty_like(pop)
        new[:elite] = pop[order[:elite]]

        # Sorteios da geração inteira feitos de uma vez (bem mais rápido em numpy).
        cand = rng.integers(0, size, size=(2 * m, tournament))
        winners = cand[np.arange(2 * m), np.argmin(lengths[cand], axis=1)]
        cx_cuts = np.sort(rng.integers(0, n + 1, size=(m, 2)), axis=1)
        do_cx = rng.random(m) < crossover_rate
        mut_cuts = np.sort(rng.integers(0, n + 1, size=(m, 2)), axis=1)
        do_mut = rng.random(m) < mutation_rate

        for i in range(m):
            child = new[elite + i]
            p1 = pop[winners[2 * i]]
            if do_cx[i]:
                order_crossover(p1, pop[winners[2 * i + 1]], cx_cuts[i, 0], cx_cuts[i, 1], child, used)
            else:
                child[:] = p1
            if do_mut[i]:
                a, b = mut_cuts[i]
                child[a:b] = child[a:b][::-1]
        pop = new
    return pop, tour_lengths(pop, dist)


# ---------------------------------------------------------------- ilha

class IslandGA:
    """Estado de uma ilha: subpopulação + parâmetros. Determinística por (seed, ilha, época)."""

    def __init__(self, spec: dict, island_id: int, population: np.ndarray | None = None,
                 dist: np.ndarray | None = None):
        self.spec = spec
        self.island_id = island_id
        self.dist = dist if dist is not None else distance_matrix(spec["coords"])
        self.n_cities = self.dist.shape[0]
        self.size = island_pop_size(spec)
        if population is None or len(population) == 0:
            rng = np.random.default_rng([int(spec["seed"]), island_id, 1_000_000])
            population = random_population(self.n_cities, self.size, rng)
        self.pop = np.array(population, dtype=POP_DTYPE)
        self.lengths = tour_lengths(self.pop, self.dist)
        self.compute_seconds = 0.0
        self.generations = 0

    def integrate(self, immigrants: np.ndarray) -> None:
        """Imigrantes substituem os piores indivíduos da ilha."""
        if immigrants is None or len(immigrants) == 0:
            return
        k = min(len(immigrants), self.size - 1)
        worst = np.argsort(self.lengths)[-k:]
        self.pop[worst] = immigrants[:k]
        self.lengths[worst] = tour_lengths(self.pop[worst], self.dist)

    def run_epoch(self, epoch: int) -> float:
        s = self.spec
        rng = np.random.default_rng([int(s["seed"]), self.island_id, epoch])
        t0 = time.perf_counter()
        self.pop, self.lengths = evolve(
            self.pop, self.dist, int(s["gens_per_epoch"]), rng,
            crossover_rate=float(s["crossover_rate"]), mutation_rate=float(s["mutation_rate"]),
            tournament=int(s["tournament"]), elite=int(s["elite"]),
        )
        if s.get("local_search"):
            b = int(np.argmin(self.lengths))
            improved = two_opt(self.pop[b], self.dist)
            self.pop[b] = improved
            self.lengths[b] = tour_lengths(improved[None, :], self.dist)[0]
        self.compute_seconds += time.perf_counter() - t0
        self.generations += int(s["gens_per_epoch"])
        return self.best_length()

    def best_index(self) -> int:
        return int(np.argmin(self.lengths))

    def best_length(self) -> float:
        return float(self.lengths.min())

    def best_tour(self) -> np.ndarray:
        return self.pop[self.best_index()].copy()

    def migrants(self, count: int) -> np.ndarray:
        order = np.argsort(self.lengths)
        return self.pop[order[:max(1, count)]].copy()


def run_sequential(spec: dict, progress=None) -> dict:
    """Linha de base sem rede: uma única ilha com a população total, mesmas épocas/gerações."""
    local = dict(spec, islands=1)
    island = IslandGA(local, 0)
    t0 = time.perf_counter()
    for epoch in range(int(local["epochs"])):
        best = island.run_epoch(epoch)
        if progress:
            progress(epoch + 1, best)
    return {
        "best_length": island.best_length(),
        "best_tour": island.best_tour().tolist(),
        "wall_seconds": time.perf_counter() - t0,
        "compute_seconds": island.compute_seconds,
    }
