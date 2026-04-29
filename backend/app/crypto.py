from __future__ import annotations

from pathlib import Path
from typing import Protocol


class Cipher(Protocol):
    def encrypt(self, value: str) -> bytes:
        ...

    def decrypt(self, value: bytes) -> str:
        ...


class FernetCipher:
    """Small wrapper so tests can inject a fake cipher without cryptography."""

    def __init__(self, fernet: object) -> None:
        self._fernet = fernet

    @classmethod
    def from_key_file(cls, path: Path) -> "FernetCipher":
        from cryptography.fernet import Fernet

        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            key = path.read_bytes()
        else:
            key = Fernet.generate_key()
            path.write_bytes(key)
            path.chmod(0o600)
        return cls(Fernet(key))

    def encrypt(self, value: str) -> bytes:
        return self._fernet.encrypt(value.encode("utf-8"))

    def decrypt(self, value: bytes) -> str:
        return self._fernet.decrypt(value).decode("utf-8")
