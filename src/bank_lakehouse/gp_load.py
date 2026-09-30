"""Загрузка выгрузок из HDFS в Greenplum через PXF.

Каждая выгрузка фактов (каталог export_id=N) грузится одной транзакцией: внешняя
таблица PXF -> stg.fact_card_txn_delta и запись в meta.loaded_exports. Сегменты
Greenplum читают Parquet из HDFS параллельно, мастер данные не пропускает через себя.
Упавшая загрузка откатывается целиком и повторяется при следующем запуске.
"""

from __future__ import annotations

import logging
import re
import time

import httpx
import psycopg
from psycopg import sql

log = logging.getLogger(__name__)

FACT_DIR = "/data/export/fact_card_txn"
DIMS_DIR = "/data/export/dims"

FACT_COLUMNS = [
    ("txn_id", "bigint"),
    ("txn_ts", "timestamptz"),
    ("txn_date", "date"),
    ("card_id", "bigint"),
    ("account_id", "bigint"),
    ("client_id", "bigint"),
    ("product", "text"),
    ("merchant_id", "bigint"),
    ("mcc", "text"),
    ("merchant_country", "text"),
    ("amount", "numeric(14,2)"),
    ("currency", "text"),
    ("fx_rate", "numeric(12,4)"),
    ("amount_kzt", "numeric(18,2)"),
    ("status", "text"),
    ("is_deleted", "boolean"),
    ("version", "bigint"),
    ("batch_id", "bigint"),
]

_META = [("version", "bigint"), ("is_deleted", "boolean"), ("updated_at", "timestamptz")]
DIM_COLUMNS = {
    "clients": [
        ("client_id", "bigint"),
        ("birth_year", "int"),
        ("city", "text"),
        ("segment", "text"),
        ("created_at", "timestamptz"),
        *_META,
    ],
    "accounts": [
        ("account_id", "bigint"),
        ("client_id", "bigint"),
        ("currency", "text"),
        ("status", "text"),
        ("opened_at", "timestamptz"),
        ("closed_at", "timestamptz"),
        *_META,
    ],
    "cards": [
        ("card_id", "bigint"),
        ("account_id", "bigint"),
        ("product", "text"),
        ("status", "text"),
        ("issued_at", "timestamptz"),
        ("expires_at", "date"),
        *_META,
    ],
    "merchants": [
        ("merchant_id", "bigint"),
        ("name", "text"),
        ("mcc", "text"),
        ("city", "text"),
        ("country", "text"),
        *_META,
    ],
}

DELTA_DDL = """
create table if not exists stg.fact_card_txn_delta (
    {cols},
    export_id bigint not null
)
-- колоночное хранение со сжатием: таблица только дописывается и читается целиком
with (appendoptimized = true, orientation = column, compresstype = zstd, compresslevel = 1)
distributed by (txn_id)
"""


def _cols(columns: list[tuple[str, str]]) -> str:
    return ",\n    ".join(f"{c} {t}" for c, t in columns)


def external_ddl(name: str, columns: list[tuple[str, str]], hdfs_path: str) -> str:
    # в LOCATION путь без ведущего слеша: pxf://<путь>?PROFILE=...
    return f"""
create external table {name} (
    {_cols(columns)}
)
location ('pxf://{hdfs_path.lstrip("/")}?PROFILE=hdfs:parquet')
format 'custom' (formatter = 'pxfwritable_import')
"""


def list_exports(webhdfs_url: str) -> list[int]:
    r = httpx.get(f"{webhdfs_url}/webhdfs/v1{FACT_DIR}", params={"op": "LISTSTATUS"}, timeout=30)
    if r.status_code == 404:
        return []
    r.raise_for_status()
    ids = []
    for st in r.json()["FileStatuses"]["FileStatus"]:
        m = re.fullmatch(r"export_id=(\d+)", st["pathSuffix"])
        if st["type"] == "DIRECTORY" and m:
            ids.append(int(m.group(1)))
    return sorted(ids)


def ensure_objects(conn: psycopg.Connection) -> None:
    with conn.transaction():
        conn.execute(DELTA_DDL.format(cols=_cols(FACT_COLUMNS)))
        # внешние таблицы справочников смотрят на постоянный каталог, данные в нём
        # полностью заменяются каждой выгрузкой справочников
        for name, cols in DIM_COLUMNS.items():
            conn.execute(f"drop external table if exists stg.ext_{name}")
            conn.execute(external_ddl(f"stg.ext_{name}", cols, f"{DIMS_DIR}/{name}"))


def load_export(conn: psycopg.Connection, export_id: int) -> int:
    ext = f"stg.ext_fact_{export_id}"
    cols = ", ".join(c for c, _ in FACT_COLUMNS)
    t0 = time.monotonic()
    with conn.transaction():
        conn.execute(f"drop external table if exists {ext}")
        conn.execute(external_ddl(ext, FACT_COLUMNS, f"{FACT_DIR}/export_id={export_id}"))
        cur = conn.execute(
            sql.SQL("insert into stg.fact_card_txn_delta ({cols}, export_id) select {cols}, %s from {ext}").format(
                cols=sql.SQL(cols), ext=sql.SQL(ext)
            ),
            (export_id,),
        )
        rows = cur.rowcount
        conn.execute(f"drop external table {ext}")
        conn.execute(
            "insert into meta.loaded_exports (export_id, rows, seconds) values (%s, %s, %s)",
            (export_id, rows, round(time.monotonic() - t0, 1)),
        )
    log.info("выгрузка %s: %s строк за %.1f с", export_id, f"{rows:,}", time.monotonic() - t0)
    return rows


def run(gp_dsn: str, webhdfs_url: str) -> dict[str, int]:
    with psycopg.connect(gp_dsn, autocommit=True) as conn:
        ensure_objects(conn)
        loaded = {r[0] for r in conn.execute("select export_id from meta.loaded_exports")}
        new = [e for e in list_exports(webhdfs_url) if e not in loaded]
        total = sum(load_export(conn, e) for e in new)
        conn.execute("analyze stg.fact_card_txn_delta")
    return {"exports": len(new), "rows": total}
