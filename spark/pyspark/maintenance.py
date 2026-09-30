"""Ежедневное обслуживание озера.

ods (Iceberg, merge-on-read): каждый часовой MERGE оставляет файлы удалений и
мелкие файлы данных. Без обслуживания чтение ods замедляется с каждым часом, а
снимки и неиспользуемые файлы копятся в HDFS.

raw (Hive, Parquet): каждый запуск cdc_to_raw пишет новый файл в партицию дня.
Вчерашние партиции переписываются одним файлом на таблицу — классическая борьба
с мелкими файлами, от которых страдает NameNode (метаданные каждого файла в памяти).
"""

from __future__ import annotations

import argparse
import datetime as dt

from pyspark.sql import SparkSession

from tables import TABLES


def compact_ods(spark: SparkSession, table: str, keep_days: int) -> None:
    name = f"ods.{table}"
    # Сливает мелкие файлы и применяет накопленные удаления к данным. Когда файл
    # данных переписан, его файл удалений ссылается в пустоту («висячий»), но сам
    # из метаданных не уходит — remove-dangling-deletes убирает такие.
    spark.sql(
        f"CALL spark_catalog.system.rewrite_data_files(table => '{name}', "
        "options => map('min-input-files', '5', 'delete-file-threshold', '1', 'remove-dangling-deletes', 'true'))"
    ).show(truncate=False)
    spark.sql(f"CALL spark_catalog.system.rewrite_position_delete_files(table => '{name}')").show(truncate=False)
    older = (dt.datetime.now() - dt.timedelta(days=keep_days)).strftime("%Y-%m-%d %H:%M:%S")
    # старые снимки не нужны для чтения, но держат файлы; time travel остаётся на keep_days
    spark.sql(
        f"CALL spark_catalog.system.expire_snapshots(table => '{name}', "
        f"older_than => TIMESTAMP '{older}', retain_last => 5)"
    ).show(truncate=False)


def compact_raw(spark: SparkSession, day: str) -> None:
    parts = spark.sql(f"SHOW PARTITIONS raw.cdc_events PARTITION (load_dt='{day}')").collect()
    for row in parts:
        spec = dict(kv.split("=") for kv in row[0].split("/"))
        where = f"tbl = '{spec['tbl']}' AND load_dt = '{day}'"
        # input_file_name() недетерминирован, внутрь агрегата Spark его не пускает
        files = spark.sql(f"SELECT input_file_name() AS f FROM raw.cdc_events WHERE {where}").distinct().count()
        if files <= 1:
            continue
        # Перезаписать партицию, читая из неё же, Spark не даёт даже из кэша: план
        # всё равно ссылается на таблицу. Поэтому сначала копия во временный каталог.
        tmp = f"hdfs://namenode:8020/tmp/raw_compact/{spec['tbl']}/{day}"
        cols = [c for c in spark.table("raw.cdc_events").columns if c not in ("tbl", "load_dt")]
        before = spark.table("raw.cdc_events").where(where).count()
        spark.table("raw.cdc_events").where(where).select(*cols).coalesce(1).write.mode("overwrite").parquet(tmp)
        spark.read.parquet(tmp).createOrReplaceTempView("part")
        spark.sql(
            f"INSERT OVERWRITE TABLE raw.cdc_events PARTITION (tbl = '{spec['tbl']}', load_dt = '{day}') "
            f"SELECT {', '.join(cols)} FROM part"
        )
        after = spark.table("raw.cdc_events").where(where).count()
        if after != before:
            raise RuntimeError(f"raw {spec['tbl']} {day}: было {before} строк, стало {after}")
        fs = spark._jvm.org.apache.hadoop.fs.Path(tmp).getFileSystem(spark._jsc.hadoopConfiguration())
        fs.delete(spark._jvm.org.apache.hadoop.fs.Path(tmp), True)
        print(f"raw {spec['tbl']} {day}: {files} файлов -> 1")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep-days", type=int, default=3)
    parser.add_argument("--raw-day", default=(dt.date.today() - dt.timedelta(days=1)).isoformat())
    args = parser.parse_args()

    spark = SparkSession.builder.appName("maintenance").enableHiveSupport().getOrCreate()
    for t in TABLES:
        compact_ods(spark, t, args.keep_days)
    compact_raw(spark, args.raw_day)
    spark.stop()


if __name__ == "__main__":
    main()
