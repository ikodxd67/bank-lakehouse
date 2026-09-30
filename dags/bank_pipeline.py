"""DAG-и хранилища.

bank_hourly       CDC -> raw -> ods -> факт (Scala) -> справочники -> Greenplum -> dbt -> сверка
bank_maintenance  ежедневно: сжатие ods и raw, снимки Iceberg, VACUUM факта в Greenplum
bank_initial_load вручную: первая выгрузка источника и полная сборка хранилища

Spark запускается на YARN в режиме cluster: драйвер работает в кластере, а задача
Airflow только ждёт завершения spark-submit. Логи драйвера — `yarn logs -applicationId`.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

BANK = os.environ.get("BANK_PROJECT_DIR", "/opt/bank")
PYSPARK = f"{BANK}/spark/pyspark"
FACT_JAR = f"{BANK}/spark/scala/target/scala-2.12/bank-fact_2.12-0.1.0.jar"
DBT = (
    f"cd {BANK}/dbt && /opt/dbt-venv/bin/dbt {{cmd}} --profiles-dir . "
    "--target-path /tmp/dbt-target --log-path /tmp/dbt-logs"
)

DEFAULTS = {
    "owner": "dwh",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Провайдер Spark по умолчанию сохраняет id приложения YARN в состоянии задачи
# (durable): если воркер Airflow упадёт, повтор задачи не отправит Spark-задачу
# второй раз, а дождётся уже запущенной. Статус берётся из REST API ResourceManager.
SPARK_COMMON = {"conn_id": "spark_yarn", "yarn_track_via_rm_api": True}


def pyspark(task_id: str, script: str, args: list[str] | None = None, py_files: str | None = None):
    return SparkSubmitOperator(
        task_id=task_id,
        **SPARK_COMMON,
        application=f"{PYSPARK}/{script}",
        py_files=py_files,
        application_args=args or [],
        name=f"airflow_{task_id}",
    )


def fact_export(task_id: str, args: list[str] | None = None):
    return SparkSubmitOperator(
        task_id=task_id,
        **SPARK_COMMON,
        application=FACT_JAR,
        java_class="bank.FactCardTxn",
        application_args=args or [],
        name=f"airflow_{task_id}",
    )


@task
def gp_load() -> dict:
    from bank_lakehouse import gp_load as loader
    from bank_lakehouse.config import Settings

    s = Settings.from_env()
    return loader.run(s.gp_dsn, s.webhdfs_url)


@task
def reconcile() -> int:
    """Падает, если в «устоявшихся» днях (старше 8 дней) источник и хранилище разошлись."""
    from datetime import date

    from bank_lakehouse import reconcile as rec
    from bank_lakehouse.config import Settings

    s = Settings.from_env()
    diffs = rec.run(s.source_dsn, s.gp_dsn, date(2025, 1, 1))
    broken = [d for d in diffs if not d.settling]
    for d in diffs:
        print(d)
    if broken:
        raise ValueError(f"расхождение источника и хранилища за {len(broken)} дн.: {[str(d.day) for d in broken]}")
    return len(diffs)


@dag(
    schedule="@hourly",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
    tags=["bank"],
)
def bank_hourly():
    cdc = pyspark("cdc_to_raw", "cdc_to_raw.py")
    ods = pyspark("raw_to_ods", "raw_to_ods.py", py_files=f"{PYSPARK}/tables.py")
    fact = fact_export("fact_export")
    dims = pyspark("export_dims", "export_dims.py")
    dbt = BashOperator(task_id="dbt_build", bash_command=DBT.format(cmd="build"))
    cdc >> ods >> [fact, dims]
    loaded = gp_load()
    [fact, dims] >> loaded >> dbt >> reconcile()


@dag(
    schedule="0 3 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
    tags=["bank"],
)
def bank_maintenance():
    lake = pyspark("lake_maintenance", "maintenance.py", py_files=f"{PYSPARK}/tables.py")

    @task
    def gp_vacuum() -> None:
        # delete+insert в append-optimized таблице помечает строки удалёнными, но
        # место не освобождает; VACUUM переписывает сегментные файлы с мусором
        import psycopg

        from bank_lakehouse.config import Settings

        with psycopg.connect(Settings.from_env().gp_dsn, autocommit=True) as conn:
            conn.execute("vacuum analyze dds.fact_card_txn")

    lake >> gp_vacuum()


@dag(schedule=None, start_date=datetime(2026, 10, 1), catchup=False, default_args=DEFAULTS, tags=["bank"])
def bank_initial_load():
    """Запускать после bank cdc-register: слот репликации должен существовать раньше выгрузки."""
    load = pyspark("initial_load", "initial_load.py", py_files=f"{PYSPARK}/tables.py")
    fact = fact_export("fact_export_full", ["--full", "true"])
    dims = pyspark("export_dims", "export_dims.py")
    dbt = BashOperator(task_id="dbt_build_full", bash_command=DBT.format(cmd="build --full-refresh"))
    load >> [fact, dims]
    [fact, dims] >> gp_load() >> dbt


bank_hourly()
bank_maintenance()
bank_initial_load()
