"""Описание таблиц источника: одно на начальную выгрузку, разбор CDC и ods.

Debezium в JSON кодирует типы не так, как JDBC: date — число дней от 1970-01-01,
timestamptz — строка ISO в UTC, numeric — строка (decimal.handling.mode=string).
Поэтому у каждой колонки два представления: как она лежит в событии и как в ods.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# служебные колонки ods
META_COLUMNS = [
    ("_version", "bigint"),  # row_version строки; у удаления before.row_version + 1
    ("_is_deleted", "boolean"),
    ("_op", "string"),  # r — начальная выгрузка, c/u/d — события CDC
    ("_lsn", "bigint"),
    ("_batch_id", "bigint"),  # пакет raw, из которого пришло последнее состояние (0 — выгрузка)
    ("_loaded_at", "timestamp"),
]


@dataclass(frozen=True)
class Table:
    name: str
    pk: list[str]
    columns: list[tuple[str, str]]  # (имя, тип Spark SQL)
    # преобразование партиционирования Iceberg; None — без партиций
    partition: str | None = None
    # колонка для параллельной выгрузки по JDBC (диапазоны значений)
    jdbc_split: str | None = None
    # неизменяемая колонка, по которой MERGE отсекает старые партиции (см. raw_to_ods)
    prune_column: str | None = None
    extra_props: dict[str, str] = field(default_factory=dict)

    @property
    def ods(self) -> str:
        return f"ods.{self.name}"

    def column_names(self) -> list[str]:
        return [c for c, _ in self.columns]


TABLES: dict[str, Table] = {
    t.name: t
    for t in [
        Table(
            "clients",
            pk=["client_id"],
            columns=[
                ("client_id", "bigint"),
                ("full_name", "string"),
                ("birth_date", "date"),
                ("phone", "string"),
                ("email", "string"),
                ("city", "string"),
                ("segment", "string"),
                ("created_at", "timestamp"),
                ("updated_at", "timestamp"),
                ("row_version", "bigint"),
            ],
        ),
        Table(
            "accounts",
            pk=["account_id"],
            columns=[
                ("account_id", "bigint"),
                ("client_id", "bigint"),
                ("currency", "string"),
                ("status", "string"),
                ("opened_at", "timestamp"),
                ("closed_at", "timestamp"),
                ("updated_at", "timestamp"),
                ("row_version", "bigint"),
            ],
        ),
        Table(
            "cards",
            pk=["card_id"],
            columns=[
                ("card_id", "bigint"),
                ("account_id", "bigint"),
                ("pan_masked", "string"),
                ("product", "string"),
                ("status", "string"),
                ("issued_at", "timestamp"),
                ("expires_at", "date"),
                ("updated_at", "timestamp"),
                ("row_version", "bigint"),
            ],
        ),
        Table(
            "merchants",
            pk=["merchant_id"],
            columns=[
                ("merchant_id", "bigint"),
                ("name", "string"),
                ("mcc", "string"),
                ("city", "string"),
                ("country", "string"),
                ("updated_at", "timestamp"),
                ("row_version", "bigint"),
            ],
        ),
        Table(
            "card_transactions",
            pk=["txn_id"],
            columns=[
                ("txn_id", "bigint"),
                ("card_id", "bigint"),
                ("merchant_id", "bigint"),
                ("txn_ts", "timestamp"),
                ("amount", "decimal(14,2)"),
                ("currency", "string"),
                ("status", "string"),
                ("auth_code", "string"),
                ("created_at", "timestamp"),
                ("updated_at", "timestamp"),
                ("row_version", "bigint"),
            ],
            partition="months(txn_ts)",
            jdbc_split="txn_id",
            prune_column="txn_ts",
        ),
        Table(
            "fx_rates",
            pk=["rate_date", "currency"],
            columns=[
                ("rate_date", "date"),
                ("currency", "string"),
                ("rate_kzt", "decimal(12,4)"),
                ("updated_at", "timestamp"),
                ("row_version", "bigint"),
            ],
        ),
    ]
}


def debezium_type(spark_type: str) -> str:
    """Тип колонки внутри before/after события Debezium (JSON без схемы)."""
    if spark_type == "date":
        return "int"
    if spark_type == "timestamp" or spark_type.startswith("decimal"):
        return "string"
    return spark_type


def envelope_ddl(table: Table) -> str:
    """DDL-схема события для from_json."""
    row = ", ".join(f"`{c}`: {debezium_type(t)}" for c, t in table.columns)
    return f"before struct<{row}>, after struct<{row}>, op string, source struct<lsn: bigint, ts_ms: bigint>"


def ods_ddl(table: Table) -> str:
    cols = ",\n    ".join(f"`{c}` {t}" for c, t in table.columns + META_COLUMNS)
    part = f"\nPARTITIONED BY ({table.partition})" if table.partition else ""
    return f"""CREATE TABLE IF NOT EXISTS {table.ods} (
    {cols}
)
USING iceberg{part}
TBLPROPERTIES (
    'format-version' = '2',
    -- MERGE каждый час меняет немного строк: merge-on-read пишет файлы удалений
    -- вместо переписывания целых файлов данных; их сливает ежедневное обслуживание
    'write.merge.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read',
    'write.parquet.compression-codec' = 'zstd'
)"""
