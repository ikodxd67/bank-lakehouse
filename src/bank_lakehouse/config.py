"""Настройки из окружения. Файл .env читается, если он есть, но окружение важнее."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_DIR = Path(os.environ.get("BANK_PROJECT_DIR", Path(__file__).resolve().parents[2]))


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


@dataclass(frozen=True)
class Settings:
    source_dsn: str
    gp_dsn: str
    connect_url: str
    webhdfs_url: str

    @classmethod
    def from_env(cls) -> Settings:
        _load_dotenv(PROJECT_DIR / ".env")
        return cls(
            source_dsn=os.environ.get("BANK_SOURCE_DSN", "postgresql://bank:bank@localhost:5433/core"),
            gp_dsn=os.environ.get("BANK_GP_DSN", "postgresql://gpadmin:gpadmin@localhost:5434/dwh"),
            connect_url=os.environ.get("BANK_CONNECT_URL", "http://localhost:8083"),
            webhdfs_url=os.environ.get("BANK_WEBHDFS_URL", "http://localhost:9870"),
        )
