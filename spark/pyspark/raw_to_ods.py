"""Применение событий CDC из raw к ods (Iceberg) через MERGE.

Для каждой таблицы берутся пакеты raw новее водяного знака, у каждого ключа
остаётся последнее событие, и оно применяется, только если его версия больше
текущей в ods. Это делает загрузку идемпотентной: повторный пакет, дубль из
raw или строка, попавшая и в начальную выгрузку, и в CDC, ничего не меняют.

Удаление не стирает строку, а ставит _is_deleted: для хранилища важно, что
авторизация была и её сняли.
"""

from __future__ import annotations

import argparse
import time

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from tables import META_COLUMNS, TABLES, Table, envelope_ddl

STATE_DDL = """
CREATE TABLE IF NOT EXISTS ods._load_state (
    table_name    string,
    last_batch_id bigint,
    updated_at    timestamp
) USING iceberg
"""


def decode(col: F.Column, spark_type: str) -> F.Column:
    """Значение из JSON Debezium в тип ods."""
    if spark_type == "date":
        return F.date_add(F.lit("1970-01-01").cast("date"), col)
    if spark_type == "timestamp":
        # в событии время в UTC с суффиксом Z; to_timestamp учитывает зону из строки
        return F.to_timestamp(col)
    if spark_type.startswith("decimal"):
        return col.cast(spark_type)
    return col


def parse_events(raw: DataFrame, table: Table) -> DataFrame:
    """Строки raw -> по одной строке на ключ с последним состоянием и служебными колонками."""
    ev = raw.select(
        F.from_json("value", envelope_ddl(table)).alias("e"),
        "op", "lsn", "batch_id", "kafka_partition", "kafka_offset",
    )
    is_delete = F.col("op") == "d"
    # у удаления after пустой, данные строки лежат в before (REPLICA IDENTITY FULL)
    image = F.when(is_delete, F.col("e.before")).otherwise(F.col("e.after"))
    cols = [decode(image[c], t).alias(c) for c, t in table.columns]
    version = F.when(is_delete, image["row_version"] + 1).otherwise(image["row_version"])
    rows = ev.select(
        *cols,
        version.alias("_version"),
        is_delete.alias("_is_deleted"),
        F.col("op").alias("_op"),
        F.col("lsn").alias("_lsn"),
        F.col("batch_id").alias("_batch_id"),
        F.current_timestamp().alias("_loaded_at"),
        "kafka_offset",
    )
    # Debezium кладёт события одного ключа в одну партицию Kafka, поэтому смещение
    # задаёт их точный порядок; версия — на случай дублей из повторного пакета.
    w = Window.partitionBy(*table.pk).orderBy(F.col("_version").desc(), F.col("kafka_offset").desc())
    return rows.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn", "kafka_offset")


def merge_sql(table: Table, source_view: str, prune_from: str | None) -> str:
    on = " AND ".join(f"t.{k} = s.{k}" for k in table.pk)
    if prune_from and table.prune_column:
        # prune_column не меняется у строки (время операции), поэтому строки пакета
        # могут совпасть только с партициями не старше самой ранней из них.
        # Без этого условия MERGE читает все партиции таблицы.
        on += f" AND t.{table.prune_column} >= TIMESTAMP '{prune_from}'"
    cols = table.column_names() + [c for c, _ in META_COLUMNS]
    sets = ", ".join(f"t.{c} = s.{c}" for c in cols)
    return f"""
MERGE INTO {table.ods} t
USING {source_view} s
ON {on}
WHEN MATCHED AND s._version > t._version THEN UPDATE SET {sets}
WHEN NOT MATCHED THEN INSERT *
"""


def last_batch(spark: SparkSession, name: str) -> int:
    row = spark.sql(f"SELECT max(last_batch_id) m FROM ods._load_state WHERE table_name = '{name}'").first()
    return row.m or 0


def apply_table(spark: SparkSession, table: Table, prune: bool) -> dict:
    since = last_batch(spark, table.name)
    # load_dt — партиция raw по дню загрузки: отсекает старые дни, не читая их
    since_dt = time.strftime("%Y-%m-%d", time.localtime(since / 1000 - 86400)) if since else "1970-01-01"
    raw = spark.table("raw.cdc_events").where(
        (F.col("tbl") == table.name) & (F.col("load_dt") >= since_dt) & (F.col("batch_id") > since)
    )
    events = parse_events(raw, table).cache()
    stats = events.agg(
        F.count("*").alias("n"),
        F.max("_batch_id").alias("max_batch"),
        F.min(table.prune_column).alias("min_prune") if table.prune_column else F.lit(None).alias("min_prune"),
    ).first()
    if not stats.n:
        events.unpersist()
        return {"table": table.name, "keys": 0}

    events.createOrReplaceTempView("cdc_batch")
    t0 = time.monotonic()
    prune_from = str(stats.min_prune) if prune and stats.min_prune else None
    spark.sql(merge_sql(table, "cdc_batch", prune_from))
    merge_s = time.monotonic() - t0
    # Водяной знак пишется после MERGE отдельным коммитом. Если упасть между ними,
    # следующий запуск применит тот же пакет ещё раз — и ничего не изменит.
    spark.sql(
        f"INSERT INTO ods._load_state VALUES ('{table.name}', {stats.max_batch}, current_timestamp())"
    )
    events.unpersist()
    return {"table": table.name, "keys": stats.n, "merge_s": round(merge_s, 1)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", default=",".join(TABLES))
    parser.add_argument("--no-prune", action="store_true", help="MERGE без отсечения партиций (для замера)")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("raw_to_ods").enableHiveSupport().getOrCreate()
    spark.sql(STATE_DDL)
    for name in args.tables.split(","):
        print(apply_table(spark, TABLES[name], prune=not args.no_prune))
    spark.stop()


if __name__ == "__main__":
    main()
