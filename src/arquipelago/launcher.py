"""Executável do teste distribuído (duplo clique) e simulação local.

Arquitetura do executável
    processo supervisor (esta tela)  --stdin/gRPC local-->  processo do nó
O supervisor mostra o log do cluster, aceita comandos do teclado e reinicia o
processo do nó quando ele "cai" durante o teste de falhas: a falha é real
(o processo morre na hora, sem avisar ninguém) e o reinício simula a
recuperação da máquina.

Modos
    lan    (padrão do executável) um nó por computador; os computadores se
           acham sozinhos por broadcast UDP; o teste começa com ENTER em
           qualquer um deles ou sozinho após alguns segundos sem novos nós.
    local  N nós nesta mesma máquina (para ensaiar sem outros computadores).
"""

import json
import os
import random
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime
from queue import Empty, Queue

from . import config
from .netutil import local_ipv4s, primary_ip

FROZEN = getattr(sys, "frozen", False)
CREATE_NO_WINDOW = 0x08000000

QUICK_OPTIONS = {"pop_per_island": 400, "epochs": 15, "fault_pop_per_island": 400, "fault_epochs": 60}

C = {"g": "\033[92m", "r": "\033[91m", "y": "\033[93m", "c": "\033[96m", "b": "\033[1m", "d": "\033[2m", "0": "\033[0m"}


# ---------------------------------------------------------------- console

def setup_console(title: str) -> None:
    if os.name == "nt":
        try:
            import ctypes
            k32 = ctypes.windll.kernel32
            k32.SetConsoleOutputCP(65001)
            k32.SetConsoleCP(65001)
            k32.SetConsoleTitleW(title)
            handle = k32.GetStdHandle(-11)
            mode = ctypes.c_uint32()
            if k32.GetConsoleMode(handle, ctypes.byref(mode)):
                k32.SetConsoleMode(handle, mode.value | 0x0004)  # cores ANSI
        except Exception:
            pass
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def color(text: str, c: str) -> str:
    return f"{C[c]}{text}{C['0']}"


def out(text: str = "") -> None:
    print(text, flush=True)


def app_dir() -> str:
    return os.path.dirname(sys.executable) if FROZEN else os.getcwd()


def writable_dir(name: str) -> str:
    for base in (app_dir(), os.path.join(os.path.expanduser("~"), "Documents"), os.environ.get("TEMP", ".")):
        path = os.path.join(base, name)
        try:
            os.makedirs(path, exist_ok=True)
            probe = os.path.join(path, ".teste_escrita")
            with open(probe, "w") as fh:
                fh.write("ok")
            os.remove(probe)
            return path
        except OSError:
            continue
    return os.path.abspath(name)


def port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.3)
        if s.connect_ex(("127.0.0.1", port)) == 0:
            return True
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("0.0.0.0", port))
            return False
        except OSError:
            return True


def free_port(start: int) -> int:
    for port in range(start, start + 50):
        if not port_in_use(port):
            return port
    raise OSError("nenhuma porta livre encontrada")


# ---------------------------------------------------------------- processo do nó

class NodeProcess:
    def __init__(self, node_id: int, port: int, data_dir: str, peers: list[str], lan: bool, skew: float,
                 local: bool = False):
        self.node_id = node_id
        self.port = port
        self.data_dir = data_dir
        self.peers = peers
        self.lan = lan
        self.skew = skew
        self.local = local
        self.incarnation = 0
        self.proc: subprocess.Popen | None = None
        self.addr = f"127.0.0.1:{port}"
        self._stub = None
        self.started_mono = 0.0
        self.fast_failures = 0
        self.gave_up = False

    def command(self) -> list[str]:
        base = [sys.executable] if FROZEN else [sys.executable, "-m", "arquipelago"]
        cmd = base + ["node", "--id", str(self.node_id), "--port", str(self.port), "--data-dir", self.data_dir,
                      "--skew", f"{self.skew:.3f}", "--incarnation", str(self.incarnation), "--quiet",
                      "--watch-parent", "--manual-clock-sync"]
        if self.lan:
            cmd.append("--lan")
        if self.peers:
            cmd += ["--peers", ",".join(self.peers)]
        if self.local:
            cmd += ["--advertise", "127.0.0.1"]
        return cmd

    @property
    def output_file(self) -> str:
        return os.path.join(self.data_dir, f"saida_no{self.node_id}.txt")

    def start(self) -> None:
        os.makedirs(self.data_dir, exist_ok=True)
        self.started_mono = time.monotonic()
        log = open(self.output_file, "a", encoding="utf-8")
        env = dict(os.environ)
        if not FROZEN:
            src = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            env["PYTHONPATH"] = src + os.pathsep + env.get("PYTHONPATH", "")
        flags = CREATE_NO_WINDOW if os.name == "nt" else 0
        self.proc = subprocess.Popen(self.command(), stdin=subprocess.PIPE, stdout=log, stderr=subprocess.STDOUT,
                                     env=env, creationflags=flags)

    def exit_code(self):
        return None if self.proc is None else self.proc.poll()

    def restart(self) -> None:
        self.incarnation += 1
        self.start()

    def kill(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.kill()

    def rpc(self, method: str, request, timeout: float = 1.5):
        import grpc
        from .rpc import arquipelago_pb2_grpc as pbg
        if self._stub is None:
            self._stub = pbg.ArquipelagoNodeStub(grpc.insecure_channel(self.addr, options=config.GRPC_CHANNEL_OPTIONS))
        try:
            return getattr(self._stub, method)(request, timeout=timeout)
        except Exception:
            return None

    def status(self) -> dict | None:
        from .rpc import arquipelago_pb2 as pb
        rep = self.rpc("GetStatus", pb.Empty())
        return json.loads(rep.json) if rep is not None else None

    def start_test(self, who: str, options: dict) -> tuple[bool, str]:
        from .rpc import arquipelago_pb2 as pb
        rep = self.rpc("StartTest", pb.StartTestRequest(h=pb.Header(), requested_by=who,
                                                        options_json=json.dumps(options).encode()), timeout=6)
        return (rep.ok, rep.info) if rep is not None else (False, "nó local não respondeu")

    def add_peer(self, address: str) -> None:
        from .rpc import arquipelago_pb2 as pb
        self.rpc("AddPeer", pb.PeerRequest(address=address))


# ---------------------------------------------------------------- tela

class Screen:
    """Imprime o log do cluster (replicado) sem repetir linhas, mesmo após reinícios."""

    def __init__(self):
        self.printed: set = set()
        self.reports_shown: set = set()

    def cluster_lines(self, st: dict, prefix: str = "") -> None:
        for line in st.get("cluster_log", []):
            key = (round(line["t"], 3), line["text"])
            if key in self.printed:
                continue
            self.printed.add(key)
            ts = datetime.fromtimestamp(line["t"]).strftime("%H:%M:%S")
            text = line["text"]
            if "FALHA" in text or "INJETANDO" in text:
                text = color(text, "y")
            elif "LÍDER" in text or "===" in text:
                text = color(text, "c")
            elif "CONCLUÍDO" in text:
                text = color(text, "g")
            out(f"{color(ts, 'd')} {prefix}{text}")

    def local_lines(self, st: dict) -> None:
        for line in st.get("local_log", []):
            key = ("L", st.get("incarnation"), round(line["t"], 3), line["text"])
            if key in self.printed:
                continue
            self.printed.add(key)
            ts = datetime.fromtimestamp(line["t"]).strftime("%H:%M:%S")
            out(color(f"{ts}   · [este PC] {line['text']}", "d"))

    def report(self, test: dict, save_dir: str) -> str | None:
        if not test or test.get("phase") != "done" or test["id"] in self.reports_shown:
            return None
        self.reports_shown.add(test["id"])
        text = test.get("report", "")
        out()
        for ln in text.splitlines():
            if "[  OK   ]" in ln:
                ln = ln.replace("[  OK   ]", color("[  OK   ]", "g"))
            elif "FALHOU" in ln:
                ln = color(ln, "r")
            elif "PULADO" in ln or "ATENÇÃO" in ln:
                ln = color(ln, "y")
            out(ln)
        os.makedirs(save_dir, exist_ok=True)
        path = os.path.join(save_dir, f"relatorio_teste_{test['id']}.txt")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text + "\n")
        with open(os.path.join(save_dir, f"relatorio_teste_{test['id']}.json"), "w", encoding="utf-8") as fh:
            json.dump(test, fh, ensure_ascii=False, indent=1, default=str)
        out(color(f"\nRelatório salvo em: {path}", "g"))
        self.conclusion(test, save_dir)
        return path

    @staticmethod
    def conclusion(test: dict, save_dir: str) -> None:
        """Grava a conclusão em um .txt separado e abre o arquivo (Bloco de Notas no Windows)."""
        from .report import build_conclusion
        try:
            path = os.path.join(save_dir, f"conclusao_teste_{test['id']}.txt")
            with open(path, "w", encoding="utf-8-sig") as fh:  # BOM: acentos certos no Bloco de Notas
                fh.write(build_conclusion(test))
            out(color(f"Conclusão salva em: {path} (aberta em outra janela)", "g"))
            if hasattr(os, "startfile") and not os.environ.get("ARQUIPELAGO_NAO_ABRIR"):
                os.startfile(path)
        except Exception as e:  # a conclusão é um extra: nunca derruba a tela do teste
            out(color(f"Não foi possível gerar a conclusão: {e}", "y"))


def _stdin_reader(q: Queue) -> None:
    while True:
        try:
            line = sys.stdin.readline()
        except Exception:
            return
        if line == "":
            return
        q.put(line.strip())


def _handle_crash(np_: NodeProcess, code: int) -> None:
    lived = time.monotonic() - np_.started_mono
    if code != config.CRASH_EXIT_CODE and lived < 15:
        np_.fast_failures += 1
        if np_.fast_failures >= 3:
            np_.gave_up = True
            out(color(f"\nO nó {np_.node_id} não consegue iniciar (código {code}). Últimas linhas do erro:", "r"))
            try:
                with open(np_.output_file, encoding="utf-8", errors="replace") as fh:
                    for ln in fh.readlines()[-12:]:
                        out("   " + ln.rstrip())
            except OSError:
                pass
            return
    else:
        np_.fast_failures = 0
    if code == config.CRASH_EXIT_CODE:
        out(color("\n" + "!" * 70, "y"))
        out(color(f"  FALHA SIMULADA: o processo do nó {np_.node_id} foi encerrado abruptamente (crash).", "y"))
        out(color("  Os outros computadores devem detectar a falha e continuar sem ele.", "y"))
        out(color("  Este nó será reiniciado em 5 s para mostrar a recuperação.", "y"))
        out(color("!" * 70 + "\n", "y"))
    else:
        out(color(f"\nO processo do nó {np_.node_id} terminou inesperadamente (código {code}). "
                  f"Veja {np_.data_dir}. Reiniciando em 5 s...", "r"))
    time.sleep(5)
    np_.restart()
    out(color(f"Nó {np_.node_id} reiniciado (encarnação {np_.incarnation}); entrando de novo no cluster...", "c"))


# ---------------------------------------------------------------- modo LAN

def run_lan(args) -> None:
    node_id = random.randint(1000, 999999)
    setup_console(f"Arquipélago — nó {node_id}")
    data_dir = args.data_dir or writable_dir("arquipelago_dados")
    results_dir = os.path.join(os.path.dirname(data_dir), "arquipelago_resultados")
    port = args.port or free_port(config.DEFAULT_GRPC_PORT)
    peers = [p.strip() for p in (args.peers or "").split(",") if p.strip()]
    peers_file = os.path.join(app_dir(), "peers.txt")
    if os.path.exists(peers_file):
        with open(peers_file, encoding="utf-8") as fh:
            for ln in fh:
                ln = ln.strip()
                if ln and not ln.startswith("#"):
                    peers.append(ln if ":" in ln else f"{ln}:{config.DEFAULT_GRPC_PORT}")
    skew = random.uniform(-2.5, 2.5)
    auto_start = 40.0 if args.auto_start is None else args.auto_start
    options = QUICK_OPTIONS if args.quick else {}

    out(color("=" * 74, "c"))
    out(color("  ARQUIPÉLAGO — Algoritmo Genético distribuído (TSP) — teste em rede local", "b"))
    out(color("=" * 74, "c"))
    out(f"  Este computador: {socket.gethostname()}   nó id {color(str(node_id), 'b')}   "
        f"porta gRPC {port}")
    out(f"  IPs desta máquina: {', '.join(local_ipv4s()) or primary_ip()}")
    out(f"  Relógio com desvio artificial de {skew:+.2f} s (para demonstrar o Berkeley)")
    out(f"  Dados e logs: {data_dir}")
    out()
    out("  1) Abra este mesmo programa nos outros computadores da rede.")
    out("  2) Eles se encontram sozinhos. Quando todos aparecerem, pressione ENTER em")
    out(f"     QUALQUER um deles (ou espere: começa sozinho {auto_start:.0f} s após o último entrar).")
    out("  3) Durante o teste, um computador e depois o líder vão 'cair' de propósito.")
    out("     Não feche as janelas: elas reiniciam sozinhas.")
    out(color("  Se o Windows perguntar sobre o firewall, clique em PERMITIR (redes privadas e públicas).", "y"))
    out()

    np_ = NodeProcess(node_id, port, data_dir, peers, lan=True, skew=skew)
    np_.start()
    q: Queue = Queue()
    threading.Thread(target=_stdin_reader, args=(q,), daemon=True).start()
    screen = Screen()
    last_members: tuple = ()
    last_change = time.monotonic()
    started_at = time.monotonic()
    hint_shown = False
    countdown_said = None
    test_requested = False
    try:
        while True:
            code = np_.exit_code()
            if code is not None:
                _handle_crash(np_, code)
                if np_.gave_up:
                    out("Feche esta janela, corrija o problema acima e abra o programa de novo.")
                    input()
                    return
                continue
            st = np_.status()
            if st is None:
                time.sleep(0.5)
                continue
            screen.cluster_lines(st)
            screen.local_lines(st)
            test = st.get("test")
            running = test is not None and test.get("phase") != "done"
            if screen.report(test, results_dir):
                test_requested = False
                if args.exit_when_done:
                    time.sleep(3)  # deixa os outros nós terminarem de ler o estado
                    return
                out(color("\nPressione ENTER para repetir o teste, ou feche a janela para sair.", "c"))
            members = tuple(sorted((m["id"], m["address"], m.get("host", "")) for m in st["members"]))
            if members != last_members:
                last_members = members
                last_change = time.monotonic()
                countdown_said = None
                if not running and st.get("leader_id"):
                    out(color(f"\nComputadores no cluster ({len(members)}), líder: nó {st['leader_id']}", "b"))
                    for mid, addr, host in members:
                        tag = " (este)" if mid == node_id else ""
                        tag += " [LÍDER]" if mid == st.get("leader_id") else ""
                        out(f"   nó {mid:>7}  {host:<22} {addr}{tag}")
            lobby = not running and not test_requested and (test is None or test.get("phase") == "done")
            if lobby and test is None and len(members) >= 2 and auto_start > 0:
                remaining = auto_start - (time.monotonic() - last_change)
                mark = int(max(remaining, 0) // 10)
                if remaining <= 0:
                    ok, info = np_.start_test(f"nó {node_id} (início automático)", options)
                    test_requested = ok or "andamento" in info
                elif mark != countdown_said:
                    countdown_said = mark
                    out(color(f"   (o teste começa sozinho em {remaining:.0f} s; ENTER para começar já)", "d"))
            if not hint_shown and test is None and len(members) < 2 and time.monotonic() - started_at > 25:
                hint_shown = True
                out(color("\nNenhum outro computador encontrado ainda. Verifique se:", "y"))
                out("  - o programa está aberto nos outros computadores;")
                out("  - estão na mesma rede (mesmo laboratório / Wi-Fi) e o firewall foi liberado;")
                out("  - se a rede bloqueia broadcast, DIGITE o IP de outro computador e tecle ENTER.")
            try:
                cmd = q.get_nowait()
            except Empty:
                cmd = None
            if cmd is not None:
                if cmd == "":
                    if running:
                        out("Teste já em andamento.")
                    else:
                        ok, info = np_.start_test(f"nó {node_id} (ENTER)", options)
                        out(color(("Teste iniciado: " if ok else "Não foi possível iniciar: ") + info,
                                  "g" if ok else "y"))
                        test_requested = ok
                elif cmd.lower() in ("sair", "exit", "q"):
                    break
                else:
                    addr = cmd if ":" in cmd else f"{cmd}:{config.DEFAULT_GRPC_PORT}"
                    np_.add_peer(addr)
                    out(f"Tentando contato com {addr}...")
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        np_.kill()


# ---------------------------------------------------------------- modo local

def run_local(args) -> int:
    n = max(2, args.nodes)
    setup_console(f"Arquipélago — simulação local com {n} nós")
    data_dir = args.data_dir or writable_dir("arquipelago_dados_local")
    results_dir = os.path.join(os.path.dirname(data_dir), "arquipelago_resultados")
    base = args.port or free_port(50061)
    ports = []
    p = base
    while len(ports) < n:
        p = free_port(p)
        ports.append(p)
        p += 1
    ids = random.sample(range(10, 100), n)
    peers = [f"127.0.0.1:{pt}" for pt in ports]
    out(color(f"Simulação local: {n} nós nesta máquina (ids {ids}, portas {ports[0]}–{ports[-1]})", "b"))
    procs = [NodeProcess(i, pt, os.path.join(data_dir, f"no{i}"), peers, lan=False,
                         skew=random.uniform(-2.5, 2.5), local=True) for i, pt in zip(ids, ports)]
    for pr in procs:
        pr.start()
    screen = Screen()
    options = QUICK_OPTIONS if args.quick else {}
    requested = False
    t0 = time.monotonic()
    try:
        while True:
            for pr in procs:
                code = pr.exit_code()
                if code is not None:
                    threading.Thread(target=_handle_crash, args=(pr, code), daemon=True).start()
                    pr.proc = None  # evita tratar a mesma saída duas vezes
            if any(pr.gave_up for pr in procs):
                return 3
            alive = [pr for pr in procs if pr.proc is not None]
            st = None
            for pr in alive:
                st = pr.status()
                if st and st.get("leader_id"):
                    break
            if st is None:
                time.sleep(0.5)
                continue
            screen.cluster_lines(st)
            test = st.get("test")
            if not requested and len(st["members"]) >= n:
                time.sleep(args.auto_start if args.auto_start is not None else 2.0)
                ok, info = alive[0].start_test("simulação local", options)
                requested = ok
                out(color(("Teste iniciado: " if ok else "Aguardando: ") + info, "g" if ok else "y"))
            if screen.report(test, results_dir):
                if args.exit_when_done:
                    checks = test.get("checks", [])
                    return 0 if all(c["status"] in ("OK", "PULADO") for c in checks) else 1
                out(color("Teste concluído. Ctrl+C para sair.", "c"))
            if time.monotonic() - t0 > 3600:
                return 2
            time.sleep(0.5)
    except KeyboardInterrupt:
        return 130
    finally:
        for pr in procs:
            pr.kill()


def main_mode(mode: str, args) -> None:
    if mode == "lan":
        run_lan(args)
    else:
        sys.exit(run_local(args))
