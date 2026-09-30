"""Сверка источника с хранилищем по дням: число подтверждённых операций и сумма.

Дни, где ещё возможны изменения (подтверждение через сутки, снятие холда через
7 дней), сверяются тоже, но расхождение в них — задержка, а не ошибка: такие дни
помечаются как «в пути».
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

import psycopg

SOURCE_SQL = """
select txn_ts::date, count(*), sum(amount)
from core.card_transactions
where status = 'posted' and currency = 'KZT' and txn_ts >= %(since)s
group by 1
"""

# сумма в исходной валюте, чтобы не зависеть от курса; сверяем тенговые операции
GP_SQL = """
select txn_date, count(*), sum(amount)
from dds.fact_card_txn
where status = 'posted' and not is_deleted and currency = 'KZT' and txn_date >= %(since)s
group by 1
"""


@dataclass(frozen=True)
class DayDiff:
    day: date
    source: tuple[int, Decimal]
    dwh: tuple[int, Decimal]
    settling: bool  # день ещё может меняться в источнике


def compare(
    source: dict[date, tuple[int, Decimal]],
    dwh: dict[date, tuple[int, Decimal]],
    settle_days: int = 8,
    today: date | None = None,
) -> list[DayDiff]:
    today = today or date.today()
    out = []
    for day in sorted(source.keys() | dwh.keys()):
        s = source.get(day, (0, Decimal(0)))
        d = dwh.get(day, (0, Decimal(0)))
        if s != d:
            out.append(DayDiff(day, s, d, settling=day >= today - timedelta(days=settle_days)))
    return out


def _fetch(dsn: str, query: str, since: date) -> dict[date, tuple[int, Decimal]]:
    with psycopg.connect(dsn) as conn:
        return {r[0]: (r[1], r[2]) for r in conn.execute(query, {"since": since})}


def run(source_dsn: str, gp_dsn: str, since: date) -> list[DayDiff]:
    return compare(_fetch(source_dsn, SOURCE_SQL, since), _fetch(gp_dsn, GP_SQL, since))
