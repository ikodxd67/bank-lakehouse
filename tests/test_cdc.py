from bank_lakehouse.cdc import TABLES, connector_config


def test_connector_reads_only_changes():
    c = connector_config()
    # историю забирает Spark по JDBC, коннектор только стримит изменения
    assert c["snapshot.mode"] == "no_data"
    assert c["publication.autocreate.mode"] == "disabled"
    assert c["decimal.handling.mode"] == "string"


def test_all_source_tables_are_captured():
    listed = set(connector_config()["table.include.list"].split(","))
    assert listed == {f"core.{t}" for t in TABLES}
    assert "core.card_transactions" in listed


def test_heartbeat_is_on():
    # без heartbeat слот держит WAL, пока в таблицах публикации нет изменений
    assert int(connector_config()["heartbeat.interval.ms"]) > 0
