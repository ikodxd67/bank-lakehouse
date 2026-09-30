"""Начальная выгрузка источника в ods через JDBC.

Запускать после регистрации коннектора Debezium: слот репликации уже создан, и
всё, что изменится во время выгрузки, придёт ещё и через CDC. Двойное попадание
безопасно — raw_to_ods применяет событие только с версией больше текущей.
"""

from __future__ import annotations

import argparse
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from tables import TABLES, Table, ods_ddl

JDBC_URL = "jdbc:postgresql://source-db:5432/core"
JDBC_PROPS = {"user": "bank", "password": "bank", "driver": "org.postgresql.Driver", "fetchsize": "20000"}


def read_source(spark: SparkSession, table: Table, partitions: int) -> DataFrame:
    select = ", ".join(table.column_names())
    query = f"(select {select} from core.{table.name}) src"
    if not table.jdbc_split:
        return spark.read.jdbc(JDBC_URL, query, properties=JDBC_PROPS)
    # Границы берём из самой таблицы: Spark делит [min, max] на равные диапазоны
    # и читает их параллельно отдельными запросами — по соединению на партицию.
    bounds = spark.read.jdbc(
        JDBC_URL, f"(select min({table.jdbc_split}) lo, max({table.jdbc_split}) hi from core.{table.name}) b",
        properties=JDBC_PROPS,
    ).first()
    return spark.read.jdbc(
        JDBC_URL, query, column=table.jdbc_split, lowerBound=bounds.lo, upperBound=bounds.hi + 1,
        numPartitions=partitions, properties=JDBC_PROPS,
    )


def load(spark: SparkSession, table: Table, partitions: int) -> int:
    spark.sql(ods_ddl(table))
    df = (
        read_source(spark, table, partitions)
        .withColumn("_version", F.col("row_version"))
        .withColumn("_is_deleted", F.lit(False))
        .withColumn("_op", F.lit("r"))
        .withColumn("_lsn", F.lit(None).cast("bigint"))
        .withColumn("_batch_id", F.lit(0).cast("bigint"))
        .withColumn("_loaded_at", F.current_timestamp())
    )
    # overwrite(true) заменяет содержимое таблицы целиком одним снимком Iceberg:
    # повторный запуск выгрузки не удваивает данные
    df.writeTo(table.ods).overwrite(F.lit(True))
    return spark.table(table.ods).count()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", default=",".join(TABLES))
    parser.add_argument("--partitions", type=int, default=12, help="параллельных JDBC-чтений большой таблицы")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("initial_load").enableHiveSupport().getOrCreate()
    spark.sql("CREATE DATABASE IF NOT EXISTS ods LOCATION 'hdfs://namenode:8020/warehouse/ods.db'")
    for name in args.tables.split(","):
        t0 = time.monotonic()
        rows = load(spark, TABLES[name], args.partitions)
        print(f"{name}: {rows:,} строк за {time.monotonic() - t0:.0f} с")
    spark.stop()


if __name__ == "__main__":
    main()
