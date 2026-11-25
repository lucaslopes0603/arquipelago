"""Armazenamento chave-valor replicado (estado do coordenador).

Modelo primário-backup: só o líder escreve (put/delete). Cada escrita recebe
um número de versão crescente. Os seguidores recebem:
  * delta  : entradas com versão > versão que o seguidor já confirmou;
  * snapshot completo: quando o seguidor é novo, ficou para trás de forma
    irrecuperável, ou quando muda o mandato (term) do líder.

Como a versão só cresce dentro de um mandato, um seguidor aceita um delta
apenas se sua versão atual for exatamente a `base_version` do delta e o
mandato coincidir; caso contrário pede um snapshot completo. Isso garante que
todas as réplicas passam pela mesma sequência de estados.
"""

import base64
import hashlib
import json
import threading
import zlib


class ReplicatedStore:
    def __init__(self):
        self.lock = threading.RLock()
        self.changed = threading.Condition(self.lock)
        self.term = 0
        self.version = 0
        self._entries: dict[str, tuple[int, object]] = {}
        self._tombstones: dict[str, int] = {}
        self._digest_cache: tuple[int, int, str] | None = None

    # ------------------------------------------------------------ leitura

    def get(self, key: str, default=None):
        with self.lock:
            item = self._entries.get(key)
            return default if item is None else item[1]

    def items(self, prefix: str = "") -> list[tuple[str, object]]:
        with self.lock:
            return sorted((k, v[1]) for k, v in self._entries.items() if k.startswith(prefix))

    def keys(self, prefix: str = "") -> list[str]:
        with self.lock:
            return sorted(k for k in self._entries if k.startswith(prefix))

    # ------------------------------------------------------------ escrita (líder)

    def put(self, key: str, value) -> int:
        with self.lock:
            self.version += 1
            self._entries[key] = (self.version, value)
            self._tombstones.pop(key, None)
            self.changed.notify_all()
            return self.version

    def update(self, key: str, fn, default=None):
        """Leitura-modificação-escrita atômica: value = fn(valor_atual)."""
        with self.lock:
            current = self.get(key, default)
            value = fn(current)
            self.put(key, value)
            return value

    def delete(self, key: str) -> None:
        with self.lock:
            if key in self._entries:
                self.version += 1
                del self._entries[key]
                self._tombstones[key] = self.version
                self.changed.notify_all()

    def start_term(self, term: int) -> None:
        """Novo líder: o mandato muda e os seguidores receberão snapshot completo."""
        with self.lock:
            self.term = term
            self.changed.notify_all()

    # ------------------------------------------------------------ replicação

    def encode_since(self, base_version: int) -> tuple[bytes, int, int]:
        """Delta a partir de base_version (0 = snapshot completo)."""
        with self.lock:
            sets = {k: [v, val] for k, (v, val) in self._entries.items() if v > base_version}
            dels = {k: v for k, v in self._tombstones.items() if v > base_version} if base_version else {}
            payload = {"term": self.term, "version": self.version, "set": sets, "del": dels}
            raw = json.dumps(payload, separators=(",", ":")).encode()
            return zlib.compress(raw, 1), self.term, self.version

    def apply(self, data: bytes, full: bool, base_version: int) -> tuple[bool, int]:
        """Aplica delta/snapshot no seguidor. Retorna (ok, versão_atual)."""
        payload = json.loads(zlib.decompress(data))
        with self.lock:
            if full:
                self._entries = {k: (v, val) for k, (v, val) in payload["set"].items()}
                self._tombstones = {}
            else:
                if int(payload["term"]) != self.term or base_version != self.version:
                    return False, self.version
                for k, (v, val) in payload["set"].items():
                    self._entries[k] = (v, val)
                for k in payload["del"]:
                    self._entries.pop(k, None)
            self.term = int(payload["term"])
            self.version = int(payload["version"])
            self.changed.notify_all()
            return True, self.version

    def digest(self) -> str:
        """Hash do conteúdo, para conferir se as réplicas estão idênticas."""
        with self.lock:
            if self._digest_cache and self._digest_cache[:2] == (self.term, self.version):
                return self._digest_cache[2]
            canon = json.dumps({k: v[1] for k, v in self._entries.items()}, sort_keys=True,
                               separators=(",", ":"))
            digest = hashlib.sha256(canon.encode()).hexdigest()[:16]
            self._digest_cache = (self.term, self.version, digest)
            return digest


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def unb64(text: str) -> bytes:
    return base64.b64decode(text) if text else b""
