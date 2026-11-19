"""In-memory stand-ins for BigQuery, shared by the PO Suggestion Tool tests.

``FakeTrackingBigQuery`` emulates only what the download tracker uses: the
``CREATE TABLE IF NOT EXISTS`` DDL, the idempotent ``INSERT ... WHERE NOT EXISTS``
keyed on ``(event_id, store_code)``, and the ``SELECT``. It reads the query
parameters the real client would send, so a change to the parameter shape in
po_buffer_tracking breaks these tests.
"""
from __future__ import annotations

import pandas as pd
from google.api_core import exceptions as gexc

import po_buffer_tracking as T


class FakeJob:
    def __init__(self, frame=None):
        self._frame = frame

    def result(self):
        return iter(())

    def to_dataframe(self):
        return self._frame


class FakeClock:
    """Monotonic clock the tests can advance."""

    def __init__(self, start=1000.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class FakeTrackingBigQuery:
    def __init__(self, *, can_create=True, table_exists=False, fail_inserts=False,
                 fail_selects=False):
        self.can_create = can_create
        self.table_exists = table_exists
        self.fail_inserts = fail_inserts
        self.fail_selects = fail_selects
        self.rows = []
        self.queries = []
        self.insert_calls = 0

    # -- helpers for tests
    def tracking_queries(self):
        return [q for q in self.queries if T.TRACKING_TABLE in q]

    # -- client API used by TrackingStore
    def query(self, sql, job_config=None):
        self.queries.append(sql)
        head = sql.lstrip().upper()
        if head.startswith("CREATE TABLE"):
            if not self.can_create:
                raise gexc.Forbidden("bigquery.tables.create denied")
            self.table_exists = True
            return FakeJob()
        if head.startswith("INSERT INTO"):
            self.insert_calls += 1
            if self.fail_inserts:
                raise gexc.ServiceUnavailable("backend unavailable")
            if not self.table_exists:
                raise gexc.NotFound("table missing")
            (param,) = job_config.query_parameters
            for struct in param.values:
                row = dict(struct.struct_values)
                key = (row["event_id"], row["store_code"])
                if any((r["event_id"], r["store_code"]) == key for r in self.rows):
                    continue  # WHERE NOT EXISTS
                self.rows.append(row)
            return FakeJob()
        if head.startswith("SELECT") and T.TRACKING_TABLE in sql:
            if self.fail_selects:
                raise gexc.ServiceUnavailable("backend unavailable")
            frame = pd.DataFrame(self.rows, columns=T.TRACKING_COLUMNS)
            return FakeJob(frame.sort_values("downloaded_at", ascending=False))
        raise AssertionError(f"unexpected query: {sql[:80]}")

    def get_table(self, table_id):
        if not self.table_exists:
            raise gexc.NotFound("not found")
        return object()


# ---------------------------------------------------------------------------
# Whole-app fake: inventory + master + store summary + tracking
# ---------------------------------------------------------------------------

def build_inventory_fixture():
    """Six stores in two regions. Two share a name; one has an illegal character."""
    stores = [
        # code,   name,                 region,       distributor
        ("ST001", "Toko Maju",          "JAWA BARAT", "DIST A"),
        ("ST002", "Toko Sama",          "JAWA BARAT", "DIST A"),
        ("ST003", "Toko Sama",          "JAWA BARAT", "DIST A"),
        ("ST004", "Toko Baru/Pusat",    "JAWA BARAT", "DIST B"),
        ("ST005", "Toko Lima",          "JAWA BARAT", "DIST B"),
        ("ST006", "Toko Sumatera",      "SUMATERA",   "DIST C"),
    ]
    rows = []
    for code, name, region, distributor in stores:
        for n in range(1, 4):
            rows.append(
                {
                    "store_code": code,
                    "store_name": name,
                    "region": region,
                    "distributor_g2g": distributor,
                    "product_code": f"SKU{code[-1]}{n}",
                    "product_name": f"Produk {code} nomor {n}",
                    "buffer_plan": 10.0 * n,
                    "buffer_plan_value": 50_000.0 * n,
                    "estimated_actual_stock_adj": float(n - 1),
                    "stock_date": pd.Timestamp("2026-11-18"),
                    "SO_Bulanan": 5 * n,
                    "priority_label": "Must Have SKU" if n == 1 else None,
                    "status_stok_distributor": "Stok Tersedia",
                }
            )
    return pd.DataFrame(rows)


def build_master_fixture():
    return pd.DataFrame(
        [
            {"region_g2g": "JAWA BARAT", "distributor": "DIST A"},
            {"region_g2g": "JAWA BARAT", "distributor": "DIST B"},
            {"region_g2g": "SUMATERA", "distributor": "DIST C"},
        ]
    )


class FakePoBufferBigQuery(FakeTrackingBigQuery):
    """Answers every query po_buffer.py issues, from fixtures held in memory."""

    def __init__(self, inventory=None, master=None, detail_inventory=None, **kwargs):
        """``inventory`` feeds the store summary; ``detail_inventory`` (default: the
        same data) is what the full inventory query returns. Making them differ
        simulates a summary that lists a store whose detail rows are unusable."""
        super().__init__(**kwargs)
        self.inventory = build_inventory_fixture() if inventory is None else inventory
        self.detail_inventory = detail_inventory
        self.master = build_master_fixture() if master is None else master
        self.all_queries = []

    def query(self, sql, job_config=None):
        self.all_queries.append(sql)
        if T.TRACKING_TABLE in sql:
            return super().query(sql, job_config)
        # NB: the inventory SQL itself contains "MAX(last_updated_jkt) AS ...", so the
        # most specific patterns must be tested first.
        if "AS grouped_inventory" in sql and "WITH combined_data AS" not in sql:
            detail = self.inventory if self.detail_inventory is None else self.detail_inventory
            return FakeJob(detail.copy())
        if sql.lstrip().startswith("SELECT MAX(last_updated_jkt)"):
            return FakeJob(pd.DataFrame({"last_updated_jkt": [pd.Timestamp.now("UTC").tz_localize(None)]}))
        if "master_distributor" in sql:
            return FakeJob(self.master.copy())
        if "WITH combined_data AS" in sql:
            wanted = set(job_config.query_parameters[0].values)
            inv = self.inventory[self.inventory["distributor_g2g"].isin(wanted)]
            grouped = (
                inv.groupby("store_code")
                .agg(
                    store_name=("store_name", "first"),
                    region=("region", "first"),
                    distributor=("distributor_g2g", "first"),
                    stock_date=("stock_date", "max"),
                    total_skus=("product_code", "nunique"),
                    total_buffer_qty=("buffer_plan", "sum"),
                    est_order_value=("buffer_plan_value", "sum"),
                )
                .reset_index()
                .sort_values("est_order_value", ascending=False)
            )
            return FakeJob(grouped)
        raise AssertionError(f"unexpected query: {sql[:120]}")
