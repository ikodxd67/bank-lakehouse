"""События CDC из Kafka в raw: Hive-таблица Parquet, событие хранится как есть.

Запускается по расписанию с trigger(availableNow): забирает всё, что накопилось с
прошлого запуска, и завершается. Позиция в Kafka хранится в checkpoint в HDFS.

raw хранит JSON события целиком, без разбора: новая колонка в источнике не ломает
загрузку, а разбор по схеме делает raw_to_ods.
"""

from __future__ import annotations

import argparse
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

RAW_DDL = """
CREATE TABLE IF NOT EXISTS raw.cdc_events (
    key          string,
    value        string,
    op           string,
    lsn          bigint,
    source_ts    timestamp,
    kafka_partition int,
    kafka_offset bigint,
    kafka_ts     timestamp,
    batch_id     bigint
)
PARTITIONED BY (tbl string, load_dt string)
STORED AS PARQUET
"""


def to_rows(df: DataFrame, batch_id: int) -> DataFrame:
    value = F.col("value").cast("string")
    return df.select(
        F.col("key").cast("string").alias("key"),
        value.alias("value"),
        F.get_json_object(value, "$.op").alias("op"),
        F.get_json_object(value, "$.source.lsn").cast("bigint").alias("lsn"),
        F.timestamp_millis(F.get_json_object(value, "$.source.ts_ms").cast("bigint")).alias("source_ts"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.lit(batch_id).cast("bigint").alias("batch_id"),
        # core.core.card_transactions -> card_transactions
        F.element_at(F.split(F.col("topic"), r"\."), -1).alias("tbl"),
        F.date_format(F.current_timestamp(), "yyyy-MM-dd").alias("load_dt"),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="kafka:19092")
    parser.add_argument("--checkpoint", default="hdfs://namenode:8020/checkpoints/cdc_to_raw")
    parser.add_argument("--max-offsets", type=int, default=500_000, help="событий за микропакет")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("cdc_to_raw").enableHiveSupport().getOrCreate()
    spark.sql("CREATE DATABASE IF NOT EXISTS raw LOCATION 'hdfs://namenode:8020/data/raw'")
    spark.sql(RAW_DDL)
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

    def write_batch(df: DataFrame, _: int) -> None:
        # Номер пакета — время записи в миллисекундах: растёт от запуска к запуску,
        # а номер микропакета Spark у availableNow начинается заново после сброса checkpoint.
        batch_id = int(time.time() * 1000)
        # одна партиция на таблицу: иначе каждый запуск плодит мелкие файлы по числу задач
        to_rows(df, batch_id).repartition("tbl").write.insertInto("raw.cdc_events")

    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribePattern", r"core\.core\..*")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", args.max_offsets)
        .load()
    )
    query = (
        stream.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", args.checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    for p in query.recentProgress:
        print(f"пакет {p['batchId']}: {p['numInputRows']} событий")
    spark.stop()


if __name__ == "__main__":
    main()
