"""Análise de desempenho: AG distribuído com k nós vs. execução em uma máquina.

Escalabilidade forte: população total, épocas e gerações fixas; varia o número
de nós (processos) k. Para cada k, sobe um cluster novo, envia o job com k
ilhas, mede o tempo de parede (do envio ao fim do job, relógio do líder) e o
tempo de cálculo de cada ilha. A diferença é o overhead do sistema
distribuído (comunicação, replicação, coordenação).

Também roda a versão sequencial pura (sem rede, ga.run_sequential).

Uso:
  python scripts/benchmark.py --reps 2 --out relatorio/dados   (k = 1, 2, 3, 4, 6 e 8)
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

import grpc  # noqa: E402

from arquipelago import config, ga, tsp  # noqa: E402
from arquipelago.rpc import arquipelago_pb2 as pb  # noqa: E402
from arquipelago.rpc import arquipelago_pb2_grpc as pbg  # noqa: E402


def stub(addr):
    return pbg.ArquipelagoNodeStub(grpc.insecure_channel(addr, options=config.GRPC_CHANNEL_OPTIONS))


def status(s):
    try:
        return json.loads(s.GetStatus(pb.Empty(), timeout=3).json)
    except grpc.RpcError:
        return None


def start_cluster(k, base_port, tmp):
    ports = [base_port + i for i in range(k)]
    peers = ",".join(f"127.0.0.1:{p}" for p in ports)
    env = dict(os.environ, PYTHONPATH=os.path.join(ROOT, "src"))
    procs = []
    for i, p in enumerate(ports):
        cmd = [sys.executable, "-m", "arquipelago", "node", "--id", str(i + 1), "--port", str(p),
               "--advertise", "127.0.0.1", "--peers", peers, "--data-dir", os.path.join(tmp, f"n{i + 1}"),
               "--quiet", "--no-preempt"]
        procs.append(subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    stubs = [stub(f"127.0.0.1:{p}") for p in ports]
    deadline = time.time() + 90
    while time.time() < deadline:
        for s in stubs:  # o líder é quem venceu a eleição (normalmente, mas nem sempre, o maior id)
            st = status(s)
            if st and st["role"] == "leader" and len(st["members"]) == k:
                return procs, s
        time.sleep(0.5)
    for p in procs:
        p.kill()
    raise RuntimeError(f"cluster com {k} nós não ficou pronto")


def run_job(leader, spec):
    rep = leader.SubmitJob(pb.JobRequest(h=pb.Header(), spec_json=json.dumps(spec).encode()), timeout=10)
    if not rep.ok:
        raise RuntimeError(rep.info)
    while True:
        time.sleep(0.5)
        st = status(leader)
        job = (st or {}).get("job") or {}
        if job.get("id") == rep.job_id and job.get("status") == "done":
            break
    return rep.job_id


HW_WORKER = """
import json, sys, time
sys.path.insert(0, sys.argv[1])
from arquipelago import ga
spec = json.loads(sys.argv[2])
ga.run_sequential(spec)
"""


def hardware_limit(k, coords, args, rep):
    """Controle: k processos independentes, SEM rede, cada um com 1/k da população.

    É o melhor tempo possível nesta máquina para k ilhas; a diferença para o
    sistema distribuído é o custo de comunicação/coordenação.
    """
    spec = ga.make_spec(coords, pop_size=args.pop // k, epochs=args.epochs, gens_per_epoch=args.gens,
                        seed=7 + rep)
    t0 = time.perf_counter()
    procs = [subprocess.Popen([sys.executable, "-c", HW_WORKER, os.path.join(ROOT, "src"), json.dumps(spec)])
             for _ in range(k)]
    for p in procs:
        p.wait()
    return time.perf_counter() - t0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, nargs="+", default=[1, 2, 3, 4, 6, 8])
    ap.add_argument("--reps", type=int, default=2)
    ap.add_argument("--cities", type=int, default=150)
    ap.add_argument("--pop", type=int, default=4800)
    ap.add_argument("--epochs", type=int, default=30)
    ap.add_argument("--gens", type=int, default=20)
    ap.add_argument("--out", default=os.path.join(ROOT, "relatorio", "dados"))
    ap.add_argument("--only-hw", action="store_true", help="roda só o controle de hardware e acrescenta ao JSON")
    ap.add_argument("--only-seq", action="store_true", help="refaz só a linha de base sequencial no JSON")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    coords = tsp.random_instance(args.cities, 2026)
    rows = []

    def save():
        with open(os.path.join(args.out, "benchmark.json"), "w", encoding="utf-8") as fh:
            json.dump({"params": vars(args), "rows": rows}, fh, indent=1)

    if args.only_seq:
        path = os.path.join(args.out, "benchmark.json")
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        data["rows"] = [r for r in data["rows"] if r["mode"] != "sequencial"]
        for rep in range(args.reps):
            spec = ga.make_spec(coords, pop_size=args.pop, epochs=args.epochs, gens_per_epoch=args.gens,
                                seed=7 + rep)
            r = ga.run_sequential(spec)
            data["rows"].append({"mode": "sequencial", "k": 1, "rep": rep, "wall": r["wall_seconds"],
                                 "compute_max": r["compute_seconds"], "compute_sum": r["compute_seconds"],
                                 "best": r["best_length"], "history": []})
            print(f"sequencial rep {rep}: {r['wall_seconds']:.2f}s", flush=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=1)
        return

    if args.only_hw:
        path = os.path.join(args.out, "benchmark.json")
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        data["rows"] = [r for r in data["rows"] if r["mode"] != "hardware"]
        for k in args.nodes:
            for rep in range(args.reps):
                wall = hardware_limit(k, coords, args, rep)
                data["rows"].append({"mode": "hardware", "k": k, "rep": rep, "wall": wall,
                                     "compute_max": wall, "best": 0.0})
                print(f"controle k={k} rep {rep}: {wall:.2f}s", flush=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=1)
        return

    # Sequencial puro, sem nenhum componente distribuído.
    for rep in range(args.reps):
        spec = ga.make_spec(coords, pop_size=args.pop, epochs=args.epochs, gens_per_epoch=args.gens, seed=7 + rep)
        r = ga.run_sequential(spec)
        rows.append({"mode": "sequencial", "k": 1, "rep": rep, "wall": r["wall_seconds"],
                     "compute_max": r["compute_seconds"], "compute_sum": r["compute_seconds"],
                     "best": r["best_length"], "history": []})
        print(f"sequencial rep {rep}: {r['wall_seconds']:.2f}s melhor {r['best_length']:.1f}", flush=True)
        save()

    for k in args.nodes:
        for rep in range(args.reps):
            tmp = tempfile.mkdtemp(prefix=f"bench_k{k}_")
            procs, leader = start_cluster(k, 52000, tmp)
            try:
                spec = {"label": f"bench-k{k}", "coords": coords, "islands": k, "pop_size": args.pop,
                        "epochs": args.epochs, "gens_per_epoch": args.gens, "seed": 7 + rep}
                jid = run_job(leader, spec)
                st = status(leader)
                job = st["job"]
                # detalhes por ilha estão no estado replicado; pedimos o snapshot pelo log de status
                comp = [i["compute_seconds"] for i in job["islands"]]
                rows.append({"mode": "distribuido", "k": k, "rep": rep, "wall": job["wall_seconds"],
                             "compute_max": max(comp), "compute_sum": sum(comp),
                             "best": job["best_length"], "job": jid, "islands": job["islands"]})
                print(f"k={k} rep {rep}: {job['wall_seconds']:.2f}s (cálculo máx {max(comp):.2f}s) "
                      f"melhor {job['best_length']:.1f}", flush=True)
                save()
            finally:
                for p in procs:
                    p.kill()
                for p in procs:
                    p.wait()
                time.sleep(1.0)

    # Controle: k processos independentes sem rede (limite do hardware).
    for k in args.nodes:
        for rep in range(args.reps):
            wall = hardware_limit(k, coords, args, rep)
            rows.append({"mode": "hardware", "k": k, "rep": rep, "wall": wall, "compute_max": wall, "best": 0.0})
            print(f"controle k={k} rep {rep}: {wall:.2f}s", flush=True)
            save()
    save()
    with open(os.path.join(args.out, "benchmark.csv"), "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["modo", "k", "rep", "tempo_s", "calculo_max_s", "melhor_rota"])
        for r in rows:
            w.writerow([r["mode"], r["k"], r["rep"], f"{r['wall']:.3f}", f"{r['compute_max']:.3f}",
                        f"{r['best']:.1f}"])
    print("salvo em", args.out)


if __name__ == "__main__":
    main()
