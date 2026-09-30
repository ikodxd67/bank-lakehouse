"""Правило применения CDC к ods на настоящем Spark и Iceberg (локальный каталог).

Запускается в образе кластера, где есть Spark и jar Iceberg:
    docker run --rm -v "$PWD":/w -w /w bank-lakehouse/hadoop:local \
        bash -c "pip install -q pytest && python3 -m pytest tests/spark -q"
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "spark" / "pyspark"))
from raw_to_ods import merge_sql, parse_events  # noqa: E402
from tables import TABLES, ods_ddl  # noqa: E402

TARGET = "local.ods.card_transactions"
T = TABLES["card_transactions"]


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    wh = tmp_path_factory.mktemp("wh")
    s = (
        SparkSession.builder.master("local[2]")
        .appName("test_raw_to_ods")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", f"file://{wh}")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "Asia/Almaty")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def row(txn_id: int, version: int, status: str = "authorized") -> dict:
    return {
        "txn_id": txn_id,
        "card_id": 10,
        "merchant_id": 1,
        "txn_ts": "2026-09-30T10:00:00Z",
        "amount": "100.50",
        "currency": "KZT",
        "status": status,
        "auth_code": "000001",
        "created_at": "2026-09-30T10:00:00Z",
        "updated_at": "2026-09-30T10:00:00Z",
        "row_version": version,
    }


def event(op: str, before: dict | None, after: dict | None, offset: int, lsn: int = 1) -> tuple:
    value = json.dumps({"before": before, "after": after, "op": op, "source": {"lsn": lsn, "ts_ms": 0}})
    return (value, op, lsn, 1000, 0, offset)


def raw_df(spark, events: list[tuple]):
    return spark.createDataFrame(
        events, "value string, op string, lsn bigint, batch_id bigint, kafka_partition int, kafka_offset bigint"
    )


def reset_target(spark) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {TARGET}")
    spark.sql(ods_ddl(T).replace(T.ods, TARGET))
    # начальная выгрузка: 1 — authorized v1, 2 — posted v2, 3 — authorized v1
    spark.sql(f"""
        INSERT INTO {TARGET} VALUES
        (1, 10, 1, TIMESTAMP '2026-09-30 15:00:00', 100.50, 'KZT', 'authorized', '000001',
         TIMESTAMP '2026-09-30 15:00:00', TIMESTAMP '2026-09-30 15:00:00', 1, 1, false, 'r', NULL, 0, current_timestamp()),
        (2, 10, 1, TIMESTAMP '2026-09-30 15:00:00', 100.50, 'KZT', 'posted', '000001',
         TIMESTAMP '2026-09-30 15:00:00', TIMESTAMP '2026-09-30 15:00:00', 2, 2, false, 'r', NULL, 0, current_timestamp()),
        (3, 10, 1, TIMESTAMP '2026-09-30 15:00:00', 100.50, 'KZT', 'authorized', '000001',
         TIMESTAMP '2026-09-30 15:00:00', TIMESTAMP '2026-09-30 15:00:00', 1, 1, false, 'r', NULL, 0, current_timestamp())
    """)


def apply(spark, events: list[tuple]) -> dict[int, tuple]:
    parse_events(raw_df(spark, events), T).createOrReplaceTempView("cdc_batch")
    spark.sql(merge_sql(T, "cdc_batch", prune_from="2026-09-30 00:00:00", target=TARGET))
    return {
        r.txn_id: (r.status, r._version, r._is_deleted, r._op)
        for r in spark.sql(f"SELECT txn_id, status, _version, _is_deleted, _op FROM {TARGET}").collect()
    }


def test_version_rule(spark):
    reset_target(spark)
    state = apply(
        spark,
        [
            # 1: обычное подтверждение v1 -> v2
            event("u", row(1, 1), row(1, 2, "posted"), offset=10),
            # 2: то же состояние, что уже пришло в выгрузке (v2) — ничего не меняет
            event("u", row(2, 1), row(2, 2, "posted"), offset=11),
            # 3: снятие холда — удаление получает версию before + 1
            event("d", row(3, 1), None, offset=12),
            # 4: новая операция
            event("c", None, row(4, 1), offset=13),
            # 5: два события одного ключа в пакете — побеждает последнее
            event("c", None, row(5, 1), offset=14),
            event("u", row(5, 1), row(5, 2, "reversed"), offset=15),
        ],
    )
    assert state[1] == ("posted", 2, False, "u")
    assert state[2] == ("posted", 2, False, "r")
    assert state[3] == ("authorized", 2, True, "d")
    assert state[4] == ("authorized", 1, False, "c")
    assert state[5] == ("reversed", 2, False, "u")


def test_late_event_is_ignored(spark):
    reset_target(spark)
    apply(spark, [event("u", row(1, 1), row(1, 2, "posted"), offset=20)])
    # повторный пакет из raw с тем же и более старым состоянием
    state = apply(spark, [event("c", None, row(1, 1), offset=5), event("u", row(1, 1), row(1, 2, "posted"), offset=20)])
    assert state[1] == ("posted", 2, False, "u")


def test_decoding(spark):
    ev = event("c", None, {**row(7, 1), "amount": "12345.67"}, offset=1)
    r = parse_events(raw_df(spark, [ev]), T).first()
    assert str(r.amount) == "12345.67"
    # 10:00 UTC — это 15:00 в Алматы (UTC+5)
    assert r.txn_ts.strftime("%H:%M") == "15:00"
    assert r._version == 1 and r._is_deleted is False
