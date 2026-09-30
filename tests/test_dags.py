"""Запускается в образе Airflow (см. CI): там есть Airflow и провайдер Spark."""

import pytest

pytest.importorskip("airflow")
# в Airflow 3 DagBag переехал в dag_processing, конструктор без include_examples
from airflow.dag_processing.dagbag import DagBag


@pytest.fixture(scope="module")
def bag():
    return DagBag(dag_folder="dags")


def test_no_import_errors(bag):
    assert bag.import_errors == {}


def test_hourly_chain(bag):
    dag = bag.dags["bank_hourly"]
    order = ["cdc_to_raw", "raw_to_ods", "fact_export", "gp_load", "dbt_build", "reconcile"]
    for up, down in zip(order, order[1:], strict=False):
        assert down in dag.get_task(up).downstream_task_ids, f"{up} -> {down}"
    assert "gp_load" in dag.get_task("export_dims").downstream_task_ids


def test_spark_tasks_go_to_yarn_cluster_with_tracking(bag):
    dag = bag.dags["bank_hourly"]
    for tid in ("cdc_to_raw", "raw_to_ods", "fact_export", "export_dims"):
        t = dag.get_task(tid)
        assert t._conn_id == "spark_yarn"
        assert t._yarn_track_via_rm_api is True


def test_no_catchup_and_owner(bag):
    for dag_id, dag in bag.dags.items():
        assert not dag.catchup, dag_id
        for t in dag.tasks:
            assert t.owner == "dwh", f"{dag_id}.{t.task_id}"
