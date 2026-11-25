"""Descoberta automática de nós na rede local via broadcast UDP.

Cada nó anuncia a cada segundo {app, cluster, id, porta gRPC, líder, mandato}
para 255.255.255.255 (por cada placa de rede) e para o broadcast /24 de cada
IP local. Quem recebe usa o IP de ORIGEM do pacote + a porta anunciada como
endereço do nó (mais confiável que o IP que o próprio nó acha que tem).

Se a rede bloquear broadcast, é possível informar IPs manualmente
(--peers, arquivo peers.txt ou digitando o IP no executável).
"""

import json
import logging
import socket
import threading
import time

from . import config
from .netutil import local_ipv4s

log = logging.getLogger("arquipelago")


class LanDiscovery(threading.Thread):
    def __init__(self, node, port: int = config.DISCOVERY_PORT):
        super().__init__(name="discovery", daemon=True)
        self.node = node
        self.port = port
        self._halt = threading.Event()
        self.seen: dict[int, tuple[str, float]] = {}
        self.error: str | None = None

    def stop(self) -> None:
        self._halt.set()

    def _payload(self) -> bytes:
        node = self.node
        return json.dumps({"app": "arquipelago", "cluster": node.cluster, "id": node.id, "port": node.port,
                           "host": node.hostname, "leader": node.leader_id or 0, "term": node.term}).encode()

    def _announce(self) -> None:
        data = self._payload()
        targets = {"255.255.255.255"}
        ips = local_ipv4s()
        for ip in ips:
            parts = ip.split(".")
            targets.add(".".join(parts[:3] + ["255"]))
        for ip in ips or ["0.0.0.0"]:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                    s.bind((ip, 0))
                    for t in targets:
                        try:
                            s.sendto(data, (t, self.port))
                        except OSError:
                            pass
            except OSError:
                pass
        # Vários nós na mesma máquina (testes): anúncio também pelo loopback.
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                s.sendto(data, ("127.255.255.255", self.port))
        except OSError:
            pass

    def run(self) -> None:
        try:
            rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            rx.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            rx.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            rx.bind(("", self.port))
            rx.settimeout(0.3)
        except OSError as e:
            self.error = f"não foi possível abrir a porta UDP {self.port}: {e}"
            log.error(self.error)
            return
        next_send = 0.0
        while not self._halt.is_set():
            now = time.monotonic()
            if now >= next_send:
                self._announce()
                next_send = now + config.DISCOVERY_INTERVAL
            try:
                data, (src_ip, _) = rx.recvfrom(8192)
                msg = json.loads(data)
            except (socket.timeout, ValueError):
                continue
            except OSError:
                continue
            if msg.get("app") != "arquipelago" or msg.get("cluster") != self.node.cluster:
                continue
            if int(msg.get("id", 0)) == self.node.id:
                continue
            self._on_peer(msg, src_ip)
        rx.close()

    def _on_peer(self, msg: dict, src_ip: str) -> None:
        node = self.node
        pid = int(msg["id"])
        address = f"{src_ip}:{int(msg['port'])}"
        first = pid not in self.seen
        self.seen[pid] = (address, time.monotonic())
        node.add_seed(address)
        if first:
            node.local_log(f"Descoberto na rede: nó {pid} ({msg.get('host', '?')}) em {address}")
        # Dois líderes independentes (nós ligados ao mesmo tempo): o de maior
        # (mandato, id) prevalece; o outro abdica e entra no cluster dele.
        other_leader = int(msg.get("leader", 0)) == pid
        if node.is_leader and other_leader and (int(msg.get("term", 0)), pid) > (node.term, node.id):
            node.step_down(f"outro líder encontrado na rede (nó {pid}, mandato {msg.get('term')})")
