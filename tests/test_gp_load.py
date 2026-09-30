import httpx

from bank_lakehouse import gp_load


def test_external_table_points_to_hdfs_path():
    ddl = gp_load.external_ddl("stg.ext_x", [("a", "bigint")], "/data/export/fact_card_txn/export_id=1")
    assert "pxf://data/export/fact_card_txn/export_id=1?PROFILE=hdfs:parquet" in ddl
    assert "pxfwritable_import" in ddl


def test_list_exports_keeps_only_export_directories(monkeypatch):
    listing = {
        "FileStatuses": {
            "FileStatus": [
                {"pathSuffix": "export_id=300", "type": "DIRECTORY"},
                {"pathSuffix": "export_id=100", "type": "DIRECTORY"},
                {"pathSuffix": "_SUCCESS", "type": "FILE"},
                {"pathSuffix": "tmp", "type": "DIRECTORY"},
            ]
        }
    }

    def fake_get(url, params, timeout):
        return httpx.Response(200, json=listing, request=httpx.Request("GET", url))

    monkeypatch.setattr(gp_load.httpx, "get", fake_get)
    assert gp_load.list_exports("http://nn:9870") == [100, 300]


def test_no_export_dir_yet(monkeypatch):
    monkeypatch.setattr(
        gp_load.httpx, "get", lambda url, params, timeout: httpx.Response(404, request=httpx.Request("GET", url))
    )
    assert gp_load.list_exports("http://nn:9870") == []


def test_fact_columns_have_no_personal_data():
    names = {c for c, _ in gp_load.FACT_COLUMNS} | {c for cols in gp_load.DIM_COLUMNS.values() for c, _ in cols}
    assert not names & {"full_name", "phone", "email", "birth_date", "pan_masked"}
