from datetime import datetime, timezone

import pytest

from master_data import (
    NO_REGION, MasterError, distributors_from_rows, list_distributors, list_regions, list_spvs,
    load_master, load_master_with_fallback, load_snapshot, skus_in_order,
)
from validation import Distributor, Sku

EVIL = "x'; DROP TABLE t; --"


def test_distributor_filter_blank_region_and_dependent_dropdowns():
    rows = [
        {"distributor_code": "D1", "distributor": "Gudang Satu", "region": "R1", "spv": "Ani"},
        {"distributor_code": "D2", "distributor": "Gudang Dua", "region": "R1", "spv": "Budi"},
        {"distributor_code": "D3", "distributor": "Gudang Tiga", "region": "R2", "spv": "Ani"},
        {"distributor_code": "D4", "distributor": "Tanpa", "region": "  ", "spv": "Cici"},
        {"distributor_code": "D5", "distributor": "Lewat", "region": "R1", "spv": "-"},
        {"distributor_code": "D6", "distributor": "Kosong", "region": "R1", "spv": ""},
        {"distributor_code": "", "distributor": "NoId", "region": "R1", "spv": "Ani"},
    ]
    dists = distributors_from_rows(rows)
    assert set(dists) == {"D1", "D2", "D3", "D4"}
    assert dists["D4"].region == NO_REGION
    assert list_regions(dists) == [NO_REGION, "R1", "R2"]
    assert list_spvs(dists, "R1") == ["Ani", "Budi"]
    assert [d.distributor_code for d in list_distributors(dists, "R1", "Ani")] == ["D1"]
    assert [d.distributor_code for d in list_distributors(dists, "R2", "Ani")] == ["D3"]


def test_missing_sku_still_listed_with_warning():
    skus, missing = skus_in_order(["TST000001", "TST000009"], [{"sku": "TST000001", "product_name": "Contoh"}])
    assert list(skus) == ["TST000001", "TST000009"]
    assert skus["TST000009"].name == "" and missing == ["TST000009"]


class Job:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return self._rows


class Client:
    def __init__(self, dist, sku):
        self.dist, self.sku, self.calls = dist, sku, []

    def query(self, sql, job_config=None):
        self.calls.append((sql, job_config))
        if "master_distributor" in sql:
            return Job(self.dist)
        return Job(self.sku)


def test_load_master_parameterizes_sku_codes_and_rejects_bad_table():
    client = Client(
        [{"distributor_code": "D1", "distributor": "Gudang", "region": "R", "spv": "Ani"}],
        [{"sku": "TST000001", "product_name": "Contoh"}],
    )
    data = load_master(client, "proj", "gt_schema", ["TST000001", EVIL])
    assert "D1" in data.distributors and data.skus["TST000001"].name == "Contoh"
    assert data.skus[EVIL].name == ""
    for sql, cfg in client.calls:
        assert EVIL not in sql and "DROP" not in sql
    sku_sql, sku_cfg = client.calls[1]
    assert "UNNEST(@codes)" in sku_sql
    assert sku_cfg.query_parameters[0].values == ["TST000001", EVIL]
    dist_sql = client.calls[0][0]
    assert "status = 'Active'" in dist_sql and "LIKE '%TPH%'" in dist_sql
    with pytest.raises(MasterError):
        load_master(Client([], []), "bad name", "gt_schema", ["TST000001"])


def _sample():
    from master_data import MasterData
    return MasterData(
        {"D1": Distributor("D1", "Gudang", "R", "Ani")},
        {"TST000001": Sku("TST000001", "Contoh")},
        ["diag"],
    )


def test_fallback_memory_disk_corrupt_and_missing(tmp_path):
    clock = lambda: datetime(2026, 10, 28, 1, 0, tzinfo=timezone.utc)
    path = tmp_path / "snap.json"
    memory = {}
    fresh = load_master_with_fallback(lambda: _sample(), snapshot_path=path, memory=memory, now=clock)
    assert fresh.stale is False and path.exists()

    def boom():
        raise MasterError("query gagal")

    stale = load_master_with_fallback(boom, snapshot_path=path, memory=memory, now=clock)
    assert stale.stale and "query gagal" in stale.error and "D1" in stale.data.distributors
    memory.clear()
    from_disk = load_master_with_fallback(boom, snapshot_path=path, memory=memory, now=clock)
    assert from_disk.stale and from_disk.data.skus["TST000001"].name == "Contoh"
    path.write_text("{not json", encoding="utf-8")
    assert load_snapshot(path) is None
    with pytest.raises(MasterError):
        load_master_with_fallback(boom, snapshot_path=tmp_path / "missing.json", memory={}, now=clock)
