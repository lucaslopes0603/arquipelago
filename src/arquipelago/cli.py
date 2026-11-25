"""Linha de comando: python -m arquipelago <comando> [opções]

Comandos:
  node     inicia um nó (servidor gRPC + papéis de líder/trabalhador)
  submit   envia um job do AG ao cluster (qualquer nó encaminha ao líder)
  status   mostra o estado de um nó
  test     dispara a bateria de testes automática
  lan      modo do executável: descoberta automática na rede + teste
  local    simula N nós nesta máquina e roda a bateria de testes
"""

import argparse
import json
import logging
import os
import random
import sys
import threading
import time

from . import config, tsp
from .rpc import arquipelago_pb2 as pb


def _setup_logging(data_dir: str, node_id: int, verbose: bool, console: bool = True) -> None:
    os.makedirs(data_dir, exist_ok=True)
    handlers = [logging.FileHandler(os.path.join(data_dir, f"no{node_id}.log"), encoding="utf-8")]
    if console:
        handlers.append(logging.StreamHandler(sys.stdout))
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO, handlers=handlers,
                        format="%(asctime)s %(levelname)s [nó %(nodeid)s] %(message)s")
    old = logging.getLogRecordFactory()

    def factory(*a, **kw):
        rec = old(*a, **kw)
        rec.nodeid = node_id
        return rec
    logging.setLogRecordFactory(factory)


def _watch_parent() -> None:
    """Encerra o nó se o processo supervisor (executável) morrer: stdin fecha."""
    def watch():
        try:
            while sys.stdin.read(1):
                pass
        except Exception:
            pass
        os._exit(0)
    threading.Thread(target=watch, daemon=True).start()


def cmd_node(args) -> None:
    from .node import Node
    node_id = args.id or random.randint(1000, 999999)
    _setup_logging(args.data_dir, node_id, args.verbose, console=not args.quiet)
    peers = [p.strip() for p in (args.peers or "").split(",") if p.strip()]
    node = Node(node_id, port=args.port, advertise=args.advertise, peers=peers, data_dir=args.data_dir,
                skew=args.skew, incarnation=args.incarnation, preempt=not args.no_preempt, lan=args.lan,
                cluster=args.cluster, auto_clock_sync=not args.manual_clock_sync)
    if args.watch_parent:
        _watch_parent()
    node.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        node.stop()


def _stub(addr: str):
    import grpc
    from .rpc import arquipelago_pb2_grpc as pbg
    return pbg.ArquipelagoNodeStub(grpc.insecure_channel(addr, options=config.GRPC_CHANNEL_OPTIONS))


def cmd_status(args) -> None:
    rep = _stub(args.addr).GetStatus(pb.Empty(), timeout=5)
    st = json.loads(rep.json)
    if args.json:
        print(json.dumps(st, indent=2, ensure_ascii=False))
        return
    print(f"nó {st['id']} ({st['host']}) {st['address']} — papel: {st['role']}, líder: {st['leader_id']}, "
          f"mandato {st['term']}, Lamport {st['lamport']}")
    print("membros:", ", ".join(f"{m['id']}@{m['address']}" for m in st["members"]))
    if st.get("job"):
        j = st["job"]
        print(f"job {j['id']} [{j['status']}] {j['progress']:.0%} melhor {j['best_length']}")
    for line in st["cluster_log"][-15:]:
        print("  ", line["text"])


def cmd_submit(args) -> None:
    coords = tsp.random_instance(args.cities, args.instance_seed)
    spec = {"label": args.label, "coords": coords, "pop_size": args.pop, "islands": args.islands,
            "epochs": args.epochs, "gens_per_epoch": args.gens, "seed": args.seed}
    stub = _stub(args.addr)
    rep = stub.SubmitJob(pb.JobRequest(h=pb.Header(), spec_json=json.dumps(spec).encode()), timeout=10)
    if not rep.ok:
        sys.exit(f"erro: {rep.info}")
    print(f"job {rep.job_id} criado")
    if not args.wait:
        return
    while True:
        time.sleep(1)
        try:
            st = json.loads(stub.GetStatus(pb.Empty(), timeout=5).json)
        except Exception:
            continue
        j = st.get("job") or {}
        if j.get("id") == rep.job_id:
            print(f"\r{j['progress']:.0%}  melhor rota: {j['best_length']}", end="", flush=True)
            if j["status"] == "done":
                print(f"\nconcluído em {j['wall_seconds']:.1f}s, melhor rota {j['best_length']:.1f}")
                return


def cmd_test(args) -> None:
    stub = _stub(args.addr)
    rep = stub.StartTest(pb.StartTestRequest(h=pb.Header(), requested_by="cli"), timeout=10)
    print(("teste iniciado: " if rep.ok else "erro: ") + rep.info)


def cmd_launcher(args) -> None:
    from . import launcher
    launcher.main_mode(args.mode, args)


def main(argv=None) -> None:
    p = argparse.ArgumentParser(prog="arquipelago", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    n = sub.add_parser("node", help="inicia um nó")
    n.add_argument("--id", type=int, default=0, help="id do nó (maior id vence a eleição); 0 = aleatório")
    n.add_argument("--port", type=int, default=config.DEFAULT_GRPC_PORT)
    n.add_argument("--peers", default="", help="endereços ip:porta de outros nós, separados por vírgula")
    n.add_argument("--advertise", default=None, help="IP anunciado aos outros nós (padrão: automático)")
    n.add_argument("--lan", action="store_true", help="descoberta automática por broadcast UDP")
    n.add_argument("--cluster", default=config.DEFAULT_CLUSTER)
    n.add_argument("--data-dir", default="dados")
    n.add_argument("--skew", type=float, default=0.0, help="desvio artificial do relógio, em segundos")
    n.add_argument("--incarnation", type=int, default=0)
    n.add_argument("--no-preempt", action="store_true", help="não retomar a liderança ao voltar (Bully modificado)")
    n.add_argument("--manual-clock-sync", action="store_true", help="1ª rodada de Berkeley só no teste")
    n.add_argument("--watch-parent", action="store_true", help=argparse.SUPPRESS)
    n.add_argument("--quiet", action="store_true", help="não imprimir log no console")
    n.add_argument("--verbose", action="store_true")
    n.set_defaults(func=cmd_node)

    s = sub.add_parser("status", help="estado de um nó")
    s.add_argument("--addr", default=f"127.0.0.1:{config.DEFAULT_GRPC_PORT}")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_status)

    j = sub.add_parser("submit", help="envia um job do AG")
    j.add_argument("--addr", default=f"127.0.0.1:{config.DEFAULT_GRPC_PORT}")
    j.add_argument("--cities", type=int, default=150)
    j.add_argument("--instance-seed", type=int, default=2026)
    j.add_argument("--pop", type=int, default=2400, help="população total (dividida entre as ilhas)")
    j.add_argument("--islands", type=int, default=0, help="0 = uma ilha por nó")
    j.add_argument("--epochs", type=int, default=30)
    j.add_argument("--gens", type=int, default=20, help="gerações por época")
    j.add_argument("--seed", type=int, default=7)
    j.add_argument("--label", default="job")
    j.add_argument("--wait", action="store_true")
    j.set_defaults(func=cmd_submit)

    t = sub.add_parser("test", help="dispara a bateria de testes")
    t.add_argument("--addr", default=f"127.0.0.1:{config.DEFAULT_GRPC_PORT}")
    t.set_defaults(func=cmd_test)

    for name, helptext in (("lan", "modo do executável (rede local)"), ("local", "simula N nós nesta máquina")):
        lp = sub.add_parser(name, help=helptext)
        lp.add_argument("--nodes", type=int, default=3)
        lp.add_argument("--port", type=int, default=0)
        lp.add_argument("--peers", default="")
        lp.add_argument("--auto-start", type=float, default=None)
        lp.add_argument("--quick", action="store_true", help="teste mais curto")
        lp.add_argument("--exit-when-done", action="store_true")
        lp.add_argument("--data-dir", default=None)
        lp.set_defaults(func=cmd_launcher, mode=name)

    args = p.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
