"""Gera as figuras do relatório a partir de relatorio/dados/benchmark.json.

  fig_tempo.pdf        tempo de parede x tempo de cálculo da ilha mais lenta
  fig_speedup.pdf      speedup e eficiência por número de nós
  fig_convergencia.pdf melhor rota por época: 1 nó x k nós
"""

import json
import os
import statistics
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "relatorio", "dados", "benchmark.json")
OUT = os.path.join(ROOT, "relatorio", "figuras")

# Cores dos gráficos: azul e laranja para as duas séries, cinzas para texto e grade.
BLUE, ORANGE = "#2a78d6", "#eb6834"
INK, INK2, GRID, REF = "#0b0b0b", "#52514e", "#e4e3df", "#8a8984"

plt.rcParams.update({
    "font.family": "serif", "font.size": 9, "axes.edgecolor": INK2, "axes.labelcolor": INK,
    "xtick.color": INK2, "ytick.color": INK2, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "legend.frameon": False,
    "lines.linewidth": 1.5, "lines.markersize": 5, "savefig.bbox": "tight",
})


def load():
    with open(DATA, encoding="utf-8") as fh:
        data = json.load(fh)
    seq = [r["wall"] for r in data["rows"] if r["mode"] == "sequencial"]
    by_k, hw = {}, {}
    for r in data["rows"]:
        if r["mode"] == "distribuido":
            by_k.setdefault(r["k"], []).append(r)
        elif r["mode"] == "hardware":
            hw.setdefault(r["k"], []).append(r["wall"])
    hw = {k: min(v) for k, v in sorted(hw.items())}
    return data, min(seq), dict(sorted(by_k.items())), hw


def best_run(rows):
    """Melhor de N: a execução de menor tempo (menos interferência do SO e do aquecimento)."""
    return min(rows, key=lambda r: r["wall"])


def fig_tempo(by_k):
    ks = list(by_k)
    wall = [best_run(by_k[k])["wall"] for k in ks]
    comp = [best_run(by_k[k])["compute_max"] for k in ks]
    fig, ax = plt.subplots(figsize=(4.2, 2.6))
    ax.plot(ks, wall, "-o", color=BLUE, label="tempo de parede (job completo)")
    ax.plot(ks, comp, "--s", color=ORANGE, label="cálculo da ilha mais lenta")
    ax.set_xlabel("número de nós (k)")
    ax.set_ylabel("tempo (s)")
    ax.set_xticks(ks)
    ax.set_ylim(bottom=0)
    ax.legend(loc="upper right")
    ax.annotate(f"{wall[0]:.1f} s".replace(".", ","), (ks[0], wall[0]), textcoords="offset points",
                xytext=(8, -4), color=INK)
    ax.annotate(f"{wall[-1]:.1f} s".replace(".", ","), (ks[-1], wall[-1]), textcoords="offset points",
                xytext=(-12, 10), color=INK)
    fig.savefig(os.path.join(OUT, "fig_tempo.pdf"))
    fig.savefig(os.path.join(OUT, "fig_tempo.png"), dpi=300)
    plt.close(fig)


def fig_speedup(seq, by_k, hw):
    """Speedup relativo: cada curva dividida pelo seu próprio tempo com k=1 (mesma sessão)."""
    ks = list(by_k)
    walls = {k: best_run(by_k[k])["wall"] for k in ks}
    sp = [walls[1] / walls[k] for k in ks]
    ef = [s / k for s, k in zip(sp, ks)]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(6.2, 2.4))
    a1.plot(ks, ks, ":", color=REF, label="ideal (linear)")
    if hw:
        hk = [k for k in ks if k in hw]
        a1.plot(hk, [hw[1] / hw[k] for k in hk], "--s", color=ORANGE,
                label="limite da máquina (sem rede)")
    a1.plot(ks, sp, "-o", color=BLUE, label="Arquipélago (gRPC)")
    a1.set_xlabel("número de nós (k)")
    a1.set_ylabel("speedup")
    a1.set_xticks(ks)
    a1.legend(loc="upper left")
    a1.annotate(f"{sp[-1]:.2f}x".replace(".", ","), (ks[-1], sp[-1]), textcoords="offset points",
                xytext=(-40, -16), color=INK)
    if hw:
        a2.plot(hk, [100 * hw[1] / hw[k] / k for k in hk], "--s", color=ORANGE)
    a2.plot(ks, [e * 100 for e in ef], "-o", color=BLUE)
    a2.set_xlabel("número de nós (k)")
    a2.set_ylabel("eficiência (%)")
    a2.set_xticks(ks)
    a2.set_ylim(0, 110)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "fig_speedup.pdf"))
    fig.savefig(os.path.join(OUT, "fig_speedup.png"), dpi=300)
    plt.close(fig)
    return sp, ef


def global_best_curve(islands):
    """Melhor rota global por época = mínimo entre as ilhas naquela época."""
    best = {}
    for isl in islands:
        for epoch, val in isl["history"]:
            best[epoch] = min(val, best.get(epoch, float("inf")))
    epochs = sorted(best)
    curve, cur = [], float("inf")
    for e in epochs:
        cur = min(cur, best[e])
        curve.append(cur)
    return epochs, curve


def fig_convergencia(by_k):
    ks = list(by_k)
    k_big = max(ks)
    one, many = by_k[1][0], by_k[k_big][0]
    fig, ax = plt.subplots(figsize=(4.2, 2.6))
    e1, c1 = global_best_curve(one["islands"])
    ek, ck = global_best_curve(many["islands"])
    ax.plot(e1, c1, "-", color=ORANGE, label="1 nó (1 população)")
    ax.plot(ek, ck, "-", color=BLUE, label=f"{k_big} nós ({k_big} ilhas + migração)")
    ax.set_xlabel("época (20 gerações cada)")
    ax.set_ylabel("melhor rota")
    ax.legend(loc="upper right")
    fig.savefig(os.path.join(OUT, "fig_convergencia.pdf"))
    fig.savefig(os.path.join(OUT, "fig_convergencia.png"), dpi=300)
    plt.close(fig)


def main():
    os.makedirs(OUT, exist_ok=True)
    data, seq, by_k, hw = load()
    fig_tempo(by_k)
    sp, ef = fig_speedup(seq, by_k, hw)
    fig_convergencia(by_k)
    seqs = sorted(r["wall"] for r in data["rows"] if r["mode"] == "sequencial")
    print(f"sequencial puro: melhor {seq:.2f} s, mediana {statistics.median(seqs):.2f} s, valores {seqs}")
    print(f"{'k':>3} {'tempo':>8} {'cálculo':>8} {'overhead':>9} {'speedup':>8} {'efic.':>6} {'rota':>9} {'sem rede':>9} {'sp.limite':>9}")
    for (k, rows), s, e in zip(by_k.items(), sp, ef):
        w = best_run(rows)["wall"]
        c = best_run(rows)["compute_max"]
        b = statistics.mean(r["best"] for r in rows)
        h = hw.get(k)
        hs = f"{h:>9.2f} {hw[1] / h:>9.2f}" if h else ""
        print(f"{k:>3} {w:>8.2f} {c:>8.2f} {100 * (w - c) / w:>8.1f}% {s:>8.2f} {100 * e:>5.0f}% {b:>9.1f} {hs}")
    print("figuras em", OUT)


if __name__ == "__main__":
    sys.exit(main())
