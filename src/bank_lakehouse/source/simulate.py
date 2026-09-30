"""Живой процессинг поверх истории: даёт поток вставок, изменений и удалений для CDC.

Правила те же, что у генератора истории: авторизация подтверждается через сутки
(90 %), часть отменяется, неподтверждённые удаляются через 7 дней.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import psycopg

log = logging.getLogger(__name__)

NEW_TXNS = """
insert into core.card_transactions
    (card_id, merchant_id, txn_ts, amount, currency, status, auth_code, created_at, updated_at)
select card_id, merchant_id, ts,
       round(case when merchant_id > %(merchants)s * 0.95 then exp(ln(40) + z)
                  when merchant_id = 1 then exp(ln(9000) + 0.9 * z)
                  else exp(ln(4500) + 1.1 * z) end::numeric, 2),
       case when merchant_id > %(merchants)s * 0.95 then 'USD' else 'KZT' end,
       'authorized', lpad(floor(random() * 1e6)::int::text, 6, '0'), ts, ts
from (
    select case when random() < 0.05 then 1 + floor(random() * 150)::bigint
                else 151 + floor((%(cards)s - 150) * power(random(), 1.3))::bigint end as card_id,
           case when random() < 0.25 then 1
                else 2 + floor((%(merchants)s - 1) * power(random(), 3))::bigint end as merchant_id,
           now() - random() * make_interval(secs => %(tick)s) as ts,
           sqrt(-2 * ln(1 - random())) * cos(2 * pi() * random()) as z
    from generate_series(1, %(n)s)
) s
"""

# 90 % авторизаций старше суток подтверждаются, у 1 % оформляется отмена, остальные ждут удаления.
POST = """
update core.card_transactions
set status = case when txn_id %% 100 = 0 then 'reversed' else 'posted' end
where status = 'authorized' and txn_ts < now() - interval '1 day' and txn_id %% 10 <> 5
"""

EXPIRE = """
delete from core.card_transactions
where status = 'authorized' and txn_ts < now() - %(hold_ttl)s::interval
"""

CLIENT_CHANGES = """
update core.clients
set phone = case when random() < 0.5 then '+7 7' || lpad(floor(random() * 1e9)::bigint::text, 9, '0') else phone end,
    city = case when random() < 0.3 then (array['Алматы','Астана','Шымкент','Караганда'])[1 + floor(random() * 4)::int]
                else city end,
    segment = case when random() < 0.2 and segment = 'mass' then 'affluent' else segment end
where client_id in (select 1 + floor(random() * %(clients)s)::bigint from generate_series(1, %(k)s))
"""

REISSUE = """
with blocked as (
    update core.cards set status = 'blocked'
    where card_id in (select 151 + floor(random() * (%(cards)s - 150))::bigint from generate_series(1, %(k)s))
      and status = 'active'
    returning account_id, product
)
insert into core.cards (card_id, account_id, pan_masked, product, status, issued_at, expires_at, updated_at)
select (select max(card_id) from core.cards) + row_number() over (), account_id,
       '4400 43** **** ' || lpad(floor(random() * 1e4)::int::text, 4, '0'),
       product, 'active', now(), (now() + interval '5 years')::date, now()
from blocked
"""

FX = """
insert into core.fx_rates (rate_date, currency, rate_kzt, updated_at)
select current_date, currency, round(rate_kzt * (1 + (random() - 0.5) * 0.01)::numeric, 4), now()
from core.fx_rates
where rate_date = (select max(rate_date) from core.fx_rates) and rate_date < current_date
on conflict do nothing
"""


@dataclass(frozen=True)
class SimParams:
    tick_seconds: float = 10.0
    txn_per_second: float = 20.0
    hold_ttl: str = "7 days"
    client_changes_per_tick: int = 3
    reissues_per_tick: int = 1


def tick(conn: psycopg.Connection, p: SimParams, sizes: dict[str, int]) -> dict[str, int]:
    counts: dict[str, int] = {}
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(NEW_TXNS, {**sizes, "n": round(p.txn_per_second * p.tick_seconds), "tick": p.tick_seconds})
        counts["inserted"] = cur.rowcount
        cur.execute(POST, {})
        counts["posted"] = cur.rowcount
        cur.execute(EXPIRE, {"hold_ttl": p.hold_ttl})
        counts["deleted"] = cur.rowcount
        cur.execute(CLIENT_CHANGES, {**sizes, "k": p.client_changes_per_tick})
        counts["clients"] = cur.rowcount
        cur.execute(REISSUE, {**sizes, "k": p.reissues_per_tick})
        counts["reissued"] = cur.rowcount
        cur.execute(FX)
        counts["fx"] = cur.rowcount
    return counts


def run(dsn: str, p: SimParams, ticks: int | None = None) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        row = conn.execute(
            "select (select max(card_id) from core.cards), (select max(merchant_id) from core.merchants),"
            " (select max(client_id) from core.clients)"
        ).fetchone()
        sizes = {"cards": row[0], "merchants": row[1], "clients": row[2]}
        n = 0
        while ticks is None or n < ticks:
            started = time.monotonic()
            counts = tick(conn, p, sizes)
            log.info("такт %d: %s", n, counts)
            n += 1
            time.sleep(max(0.0, p.tick_seconds - (time.monotonic() - started)))
