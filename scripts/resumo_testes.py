"""Resume os relatórios JSON da bateria de testes (pasta arquipelago_resultados).

Uso: python scripts/resumo_testes.py <pasta ou arquivos .json> ...
Imprime, para cada teste: nós, verificações OK, Berkeley, detecção de falha,
tempo até o novo líder, réplicas e Lamport.
"""

import glob
import json
import os
import sys


def files(args):
    for a in args:
        if os.path.isdir(a):
            yield from sorted(glob.glob(os.path.join(a, "**", "relatorio_teste_*.json"), recursive=True))
        else:
            yield a


def main():
    seen = set()
    for path in files(sys.argv[1:] or ["."]):
        with open(path, encoding="utf-8") as fh:
            t = json.load(fh)
        if t["id"] in seen:
            continue
        seen.add(t["id"])
        r = t["results"]
        checks = t.get("checks", [])
        bk = r.get("berkeley", {})
        fj = r.get("fault_job", {})
        wk, ld = fj.get("worker") or {}, fj.get("leader") or {}
        cs = r.get("consistency", {})
        lm = r.get("report", {})
        dj, bl = r.get("dist_job", {}), r.get("baseline_job", {})
        print(f"teste {t['id']} ({path})")
        print(f"  nós: {len(t.get('initial_members', []))}  verificações OK: "
              f"{sum(c['status'] == 'OK' for c in checks)}/{len(checks)}")
        for m in t.get("initial_members", []):
            print(f"    nó {m['id']} {m.get('host', '')} {m['address']} — {m.get('machine', '?')}")
        print(f"  Berkeley: {bk.get('spread_before_ms', 0):.1f} ms -> {bk.get('spread_after_ms', 0):.2f} ms")
        print(f"  AG: distribuído {dj.get('wall_seconds', 0):.1f}s, 1 máquina {bl.get('wall_seconds', 0):.1f}s, "
              f"speedup {bl.get('speedup') or 0:.2f}")
        print(f"  falha do trabalhador: detectada em {wk.get('detected_after_s')}")
        print(f"  falha do líder: novo líder {ld.get('new')} após {ld.get('failover_s')}, "
              f"ilhas retomadas: {len(ld.get('reassigned', []))}")
        print(f"  réplicas: todas iguais = {cs.get('all_match')} ({len(cs.get('replicas', [])) + 1} nós)")
        print(f"  Lamport: {lm.get('pairs')} pares ({lm.get('cross_node_pairs')} entre nós), "
              f"{lm.get('violations')} violações")


if __name__ == "__main__":
    main()
