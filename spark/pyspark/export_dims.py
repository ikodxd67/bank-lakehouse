"""Справочники из ods для Greenplum: полный снимок текущего состояния на каждый запуск.

Справочники маленькие (сотни тысяч строк), инкремент здесь дороже полной выгрузки.
Историю изменений (SCD2) строит dbt snapshot в Greenplum, сравнивая снимки.
Персональные данные клиента (ФИО, телефон, e-mail) в хранилище не уходят.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

DIMS = {
    "clients": ["client_id", "year(birth_date) AS birth_year", "city", "segment", "created_at"],
    "accounts": ["account_id", "client_id", "currency", "status", "opened_at", "closed_at"],
    "cards": ["card_id", "account_id", "product", "status", "issued_at", "expires_at"],
    "merchants": ["merchant_id", "name", "mcc", "city", "country"],
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="hdfs://namenode:8020/data/export/dims")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("export_dims").enableHiveSupport().getOrCreate()
    for name, cols in DIMS.items():
        df = spark.table(f"ods.{name}").selectExpr(
            *cols, "_version AS version", "_is_deleted AS is_deleted", "updated_at"
        )
        # один файл: PXF читает его одним фрагментом, а справочник всё равно маленький
        df.coalesce(1).write.mode("overwrite").parquet(f"{args.out}/{name}")
        print(f"{name}: {spark.read.parquet(f'{args.out}/{name}').count():,}")
    spark.stop()


if __name__ == "__main__":
    main()
