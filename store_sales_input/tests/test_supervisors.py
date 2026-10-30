import pytest

from supervisors import (
    SPV_OTHER, SupervisorData, SupervisorDirectory, SupervisorLookupError, build_options, clean_name,
    resolve_supervisor,
)


def test_clean_name_collapses_spaces_including_nbsp():
    assert clean_name("  Riko  Saputra \t") == "Riko Saputra"
    assert clean_name(None) == ""


def test_options_master_first_then_custom_sorted_other_last_without_duplicates():
    opts = build_options(["Riko Saputra", "andi", "-", ""], ["Budi", "riko saputra", "ANDI", SPV_OTHER, "Cici"])
    assert opts == ["andi", "Riko Saputra", "Budi", "Cici", SPV_OTHER]


def test_options_empty_sources_only_offer_other():
    assert build_options([], []) == [SPV_OTHER]
    assert SupervisorData().options_for(None) == [SPV_OTHER]
    assert SupervisorData().options_for("S1") == [SPV_OTHER]


def test_options_for_uses_per_store_lists():
    d = SupervisorData(master={"S1": ("Riko",)}, custom={"S1": ("Budi",), "S2": ("Cici",)})
    assert d.options_for("S1") == ["Riko", "Budi", SPV_OTHER]
    assert d.options_for("S2") == ["Cici", SPV_OTHER]


@pytest.mark.parametrize("choice,typed,expected", [
    (None, "Budi", ""),                                   # belum memilih
    ("Riko Saputra", "ignored", "Riko Saputra"),          # pilihan biasa: teks diabaikan
    (SPV_OTHER, "", ""),
    (SPV_OTHER, "   ", ""),
    (SPV_OTHER, "nama spv lainnya", ""),                  # label pilihan bukan nama orang
    (SPV_OTHER, "  Budi   Santoso ", "Budi Santoso"),
    (SPV_OTHER, "riko  SAPUTRA", "Riko Saputra"),         # ejaan pilihan yang sudah ada dipakai
    (SPV_OTHER, "x" * 150, "x" * 100),
])
def test_resolve_supervisor(choice, typed, expected):
    opts = ["Riko Saputra", SPV_OTHER]
    assert resolve_supervisor(choice, typed, opts) == expected


class FakeRow:
    def __init__(self, **kw):
        self.kw = kw

    def items(self):
        return self.kw.items()


class FakeJob:
    def __init__(self, rows):
        self.rows = rows

    def result(self):
        return self.rows


class FakeClient:
    def __init__(self, master_rows, custom_rows, fail=False):
        self.master_rows, self.custom_rows, self.fail = master_rows, custom_rows, fail
        self.sqls, self.params = [], []

    def query(self, sql, job_config=None):
        if self.fail:
            raise RuntimeError("bq down")
        self.sqls.append(sql)
        self.params.append(job_config.query_parameters[0].values)
        return FakeJob(self.master_rows if "d.spv_tph" in sql else self.custom_rows)


def directory(client):
    return SupervisorDirectory(client, "p.ds.basis", "p.ds.dist", "p.ds.tx")


def test_load_maps_rows_per_store_and_passes_ids_as_parameters():
    c = FakeClient([FakeRow(store_id="S1", names=["Riko"])], [FakeRow(store_id="S1", names=["Budi", None])])
    data = directory(c).load(["S2", "S1", "S1"])
    assert data.master == {"S1": ("Riko",)} and data.custom == {"S1": ("Budi",)}
    assert c.params == [["S1", "S2"], ["S1", "S2"]]


def test_load_without_stores_makes_no_query():
    c = FakeClient([], [])
    assert directory(c).load([]) == SupervisorData() and c.sqls == []


def test_load_failure_becomes_lookup_error():
    with pytest.raises(SupervisorLookupError, match="gagal dimuat"):
        directory(FakeClient([], [], fail=True)).load(["S1"])


def test_sql_joins_basis_to_active_distributor_on_normalised_name_and_never_inlines_ids():
    d = directory(FakeClient([], []))
    m, c = d.master_sql(), d.custom_sql()
    assert "`p.ds.basis`" in m and "`p.ds.dist`" in m and "d.status = 'Active'" in m
    assert "b.distributor_tph" in m and "d.spv_tph" in m and "@ids" in m
    assert r"[\s\x{A0}]+" in m and "UPPER(TRIM(REGEXP_REPLACE(" in m           # non-breaking space ikut dirapikan
    assert "`p.ds.tx`" in c and "t.supervisor_name" in c and "@ids" in c and "o.k = m.k" in c


def test_invalid_table_name_is_rejected():
    with pytest.raises(SupervisorLookupError):
        SupervisorDirectory(FakeClient([], []), "p.ds.basis; DROP", "p.ds.dist", "p.ds.tx")
