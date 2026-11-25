"""Utilitários de rede: IP da máquina, interfaces e correção de endereços."""

import ipaddress
import socket


def primary_ip() -> str:
    """IP da interface usada para a rota padrão (não envia nenhum pacote)."""
    for probe in ("10.255.255.255", "8.8.8.8"):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect((probe, 1))
                ip = s.getsockname()[0]
                if not ip.startswith("0."):
                    return ip
        except OSError:
            pass
    try:
        return socket.gethostbyname(socket.gethostname())
    except OSError:
        return "127.0.0.1"


def local_ipv4s() -> list[str]:
    ips = set()
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ips.add(info[4][0])
    except OSError:
        pass
    ips.add(primary_ip())
    return sorted(ip for ip in ips if not ip.startswith("127."))


def is_loopback(host: str) -> bool:
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return host == "localhost"


def split_address(address: str) -> tuple[str, int]:
    host, _, port = address.rpartition(":")
    return host.strip("[]"), int(port)


def observed_ip(grpc_peer: str | None) -> str | None:
    """Extrai o IP de context.peer() (ex.: 'ipv4:192.168.0.7:51234')."""
    if not grpc_peer or not grpc_peer.startswith("ipv4:"):
        return None
    return grpc_peer[5:].rpartition(":")[0] or None


def fix_address(advertised: str, grpc_peer: str | None) -> str:
    """Usa o IP de onde a conexão realmente veio + a porta anunciada.

    Máquinas com várias placas de rede (VirtualBox, VPN, WSL...) às vezes
    anunciam o IP errado; o IP observado pelo destinatário é o que funciona.
    """
    ip = observed_ip(grpc_peer)
    if not ip:
        return advertised
    host, port = split_address(advertised)
    if is_loopback(ip) and not is_loopback(host):
        return advertised
    return f"{ip}:{port}"
