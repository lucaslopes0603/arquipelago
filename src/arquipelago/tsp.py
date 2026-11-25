"""Problema do Caixeiro Viajante (TSP) euclidiano.

As coordenadas das cidades viajam dentro da especificação do job, então todos
os nós usam exatamente a mesma instância, independentemente da versão do numpy.
"""

import math

import numpy as np


def random_instance(n_cities: int, seed: int, size: float = 1000.0) -> list[list[float]]:
    """Cidades uniformemente distribuídas em um quadrado size x size."""
    rng = np.random.default_rng(seed)
    coords = rng.uniform(0.0, size, size=(n_cities, 2))
    return np.round(coords, 3).tolist()


def circle_instance(n_cities: int, radius: float = 500.0) -> list[list[float]]:
    """Cidades sobre um círculo: o ótimo é conhecido (perímetro do polígono)."""
    ang = np.linspace(0.0, 2 * math.pi, n_cities, endpoint=False)
    coords = np.stack([radius * np.cos(ang), radius * np.sin(ang)], axis=1)
    return coords.tolist()


def circle_optimum(n_cities: int, radius: float = 500.0) -> float:
    return n_cities * 2 * radius * math.sin(math.pi / n_cities)


def distance_matrix(coords) -> np.ndarray:
    pts = np.asarray(coords, dtype=np.float64)
    diff = pts[:, None, :] - pts[None, :, :]
    return np.sqrt((diff ** 2).sum(axis=2))


def tour_length(tour, dist: np.ndarray) -> float:
    t = np.asarray(tour, dtype=np.int64)
    return float(dist[t, np.roll(t, -1)].sum())


def is_valid_tour(tour, n_cities: int) -> bool:
    t = np.asarray(tour)
    return t.shape == (n_cities,) and np.array_equal(np.sort(t), np.arange(n_cities))
