"""Наполнение источника историей. Сама генерация — SQL в sql/source/, здесь только параметры и порядок."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date, timedelta

import psycopg

from bank_lakehouse.config import PROJECT_DIR

log = logging.getLogger(__name__)
SQL_DIR = PROJECT_DIR / "sql" / "source"


@dataclass(frozen=True)
class GenParams:
    clients: int = 200_000
    merchants: int = 20_000
    business_cards: int = 150
    business_share: float = 0.05
    per_day: int = 55_000
    growth: float = 1.2
    start: date = date(2025, 1, 1)
    until: date | None = None  # по умолчанию — сегодня: история до вчерашнего дня включительно

    def end(self) -> date:
        return self.until or date.today()


def month_ranges(start: date, until: date) -> list[tuple[date, date]]:
    """[начало, конец) по месяцам; последний месяц обрезан по until."""
    out = []
    cur = start.replace(day=1)
    while cur < until:
        nxt = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
        out.append((max(cur, start), min(nxt, until)))
        cur = nxt
    return out


def _set(cur: psycopg.Cursor, key: str, value: object) -> None:
    cur.execute("select set_config(%s, %s, false)", (f"gen.{key}", str(value)))


def migrate(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute((SQL_DIR / "001_schema.sql").read_text(encoding="utf-8"))
    log.info("схема core готова")


def generate(dsn: str, p: GenParams) -> None:
    until = p.end()
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        # генерация однопоточная: параллельные воркеры сделали бы random() недетерминированным
        cur.execute("set max_parallel_workers_per_gather = 0")
        for key in ("clients", "merchants", "business_cards", "business_share", "per_day", "growth"):
            _set(cur, key, getattr(p, key))
        _set(cur, "start", f"{p.start} 00:00+05")
        _set(cur, "until", f"{until} 00:00+05")

        t0 = time.monotonic()
        cur.execute((SQL_DIR / "010_generate_dims.sql").read_text(encoding="utf-8"))
        cur.execute("select count(*) from core.cards")
        _set(cur, "cards", cur.fetchone()[0])
        log.info("справочники готовы за %.0f с", time.monotonic() - t0)

        # identity начинается заново, иначе повторная генерация продолжит нумерацию
        cur.execute("alter table core.card_transactions alter column txn_id restart with 1")
        tx_sql = (SQL_DIR / "020_generate_transactions.sql").read_text(encoding="utf-8")
        total = 0
        for frm, to in month_ranges(p.start, until):
            t1 = time.monotonic()
            _set(cur, "from", f"{frm} 00:00+05")
            _set(cur, "to", f"{to} 00:00+05")
            cur.execute(tx_sql)
            total += cur.rowcount
            log.info("%s: %s операций за %.0f с", frm.strftime("%Y-%m"), f"{cur.rowcount:,}", time.monotonic() - t1)
        cur.execute("analyze")
    log.info("всего %s операций за %.0f с", f"{total:,}", time.monotonic() - t0)
