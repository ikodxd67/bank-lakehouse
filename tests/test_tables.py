import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark" / "pyspark"))
from tables import TABLES, debezium_type, envelope_ddl, ods_ddl


def test_debezium_types():
    assert debezium_type("date") == "int"
    assert debezium_type("timestamp") == "string"
    assert debezium_type("decimal(14,2)") == "string"
    assert debezium_type("bigint") == "bigint"


def test_envelope_has_before_after_and_lsn():
    ddl = envelope_ddl(TABLES["card_transactions"])
    assert ddl.startswith("before struct<")
    assert "after struct<" in ddl and "lsn: bigint" in ddl
    assert "`amount`: string" in ddl


def test_ods_ddl_partitioning_and_merge_on_read():
    ddl = ods_ddl(TABLES["card_transactions"])
    assert "PARTITIONED BY (months(txn_ts))" in ddl
    assert "'write.merge.mode' = 'merge-on-read'" in ddl
    assert "PARTITIONED BY" not in ods_ddl(TABLES["clients"])


def test_every_table_has_row_version():
    for t in TABLES.values():
        assert "row_version" in t.column_names(), t.name
