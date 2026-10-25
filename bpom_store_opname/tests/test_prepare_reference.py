"""Tes skrip persiapan data BD (tools/prepare_reference.py) memakai file buatan di folder sementara."""
import shutil
import sys
from pathlib import Path

import openpyxl
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tools"))
import prepare_reference as pr  # noqa: E402
import reference_data as r  # noqa: E402

DIST = r.load_distributors(ROOT / "reference" / "Daftar Distributor.xlsx")
D1, D2 = DIST[0][1], DIST[1][1]
CURRENT = r.load_catalogue(ROOT / "template" / "Contoh Excel Response.xlsx")


def xlsx(path, rows):
    wb = openpyxl.Workbook()
    ws = wb.active
    for row in rows:
        ws.append(row)
    wb.save(path)
    return path


@pytest.fixture
def sandbox(tmp_path):
    """Salinan struktur proyek (reference + template) supaya --install tidak menyentuh file asli."""
    (tmp_path / "reference").mkdir(); (tmp_path / "template").mkdir()
    shutil.copyfile(ROOT / "reference" / "Daftar Distributor.xlsx", tmp_path / "reference" / "Daftar Distributor.xlsx")
    shutil.copyfile(ROOT / "template" / "Contoh Excel Response.xlsx", tmp_path / "template" / "Contoh Excel Response.xlsx")
    return tmp_path


def run(sandbox, *argv):
    return pr.main(["--root", str(sandbox), *map(str, argv)])


HEAD = ["Distributor Name", "Store ID", "Store Name"]


def test_store_file_with_title_rows_blank_tail_and_extra_columns_is_accepted(tmp_path):
    path = xlsx(tmp_path / "s.xlsx", [["DAFTAR STORE BD"], [], ["Area", *HEAD, "Catatan"],
                                      ["x", D1, "00123", "TOKO A", "-"], ["x", D2, 77.0, "TOKO B", None], [None] * 5])
    rep, stores = pr.check_stores(path, DIST)
    assert rep.ok and [(s.store_id, s.store_name) for s in stores] == [("00123", "TOKO A"), ("77", "TOKO B")]
    assert any("2 store valid" in i for i in rep.info)


def test_store_problems_are_all_listed_with_suggestions(tmp_path):
    typo = D1[:-1] + "X"
    path = xlsx(tmp_path / "s.xlsx", [HEAD, [typo, "1", "A"], ["DISTRIBUTOR ASING", "2", "B"], [D1, "3", "C"], [D1, "3", "D"], [D1, "", "E"]])
    rep, _ = pr.check_stores(path, DIST)
    text = "\n".join(rep.errors)
    assert not rep.ok and "ganda" in text and "wajib diisi" in text
    assert "DISTRIBUTOR ASING" in text and typo in text and f"mungkin '{D1}'" in text   # saran nama mirip


def test_store_extra_sheets_are_warned(tmp_path):
    wb = openpyxl.Workbook(); wb.active.append(HEAD); wb.active.append([D1, "1", "A"]); wb.create_sheet("Lain")
    wb.save(tmp_path / "s.xlsx")
    rep, _ = pr.check_stores(tmp_path / "s.xlsx", DIST)
    assert rep.ok and any("HANYA sheet pertama" in w for w in rep.warnings)


def test_install_only_when_everything_passes(sandbox, tmp_path, capsys):
    bad = xlsx(tmp_path / "bad.xlsx", [HEAD, ["TIDAK ADA", "1", "A"]])
    assert run(sandbox, "--stores", bad, "--install") == 1
    assert not (sandbox / "reference" / "Daftar Store.xlsx").exists()
    good = xlsx(tmp_path / "good.xlsx", [HEAD, [D1, "1", "A"]])
    assert run(sandbox, "--stores", good) == 0 and not (sandbox / "reference" / "Daftar Store.xlsx").exists()   # tanpa --install
    assert run(sandbox, "--stores", good, "--install") == 0
    assert r.load_stores(sandbox / "reference" / "Daftar Store.xlsx", DIST)[0].store_id == "1"


def test_catalogue_in_template_format_replaces_sheet1_and_keeps_header_and_other_sheets(sandbox, tmp_path):
    first = CURRENT[0]
    path = xlsx(tmp_path / "c.xlsx", [
        ["PRODUCT ID", "PRODUCT NAME", "ISI (GR / ML)", "BPOM NUMBER"],
        [first.product_id, first.product_name, first.isi, first.bpom_number],
        ["ZZ-1", "PRODUK BARU", "10ml", "NA11260100001"], [None] * 4])
    assert run(sandbox, "--catalogue", path, "--install") == 0
    tpl = sandbox / "template" / "Contoh Excel Response.xlsx"
    new = r.load_catalogue(tpl)
    assert [(s.product_id, s.bpom_number) for s in new] == [(first.product_id, first.bpom_number), ("ZZ-1", "NA11260100001")]
    wb = openpyxl.load_workbook(tpl)
    assert wb.sheetnames == ["Sheet1", "Distributor Name", "SKU", "Sheet3"]            # sheet lain tidak disentuh
    assert [c.value for c in wb["Sheet1"][2]] == list(pr.val.EXPECTED_HEADERS)
    assert wb["Sheet1"]["F1"].value == "Filled by Distributor"


def test_catalogue_in_bd_format_is_completed_from_current_catalogue_and_master(tmp_path):
    first = CURRENT[0]
    path = xlsx(tmp_path / "c.xlsx", [
        ["SKU", "BPOM Product Name", "BPOM Number", "Expired Date"],
        [first.product_id, "nama BPOM bebas", "NA11299999999", None],                  # SKU dikenal -> nama + ISI dari katalog
        ["ZZ-9", "x", "NA11200000009", None],                                          # SKU baru -> dari master
        ["QQ-0", "x", "NA11200000010", None]])                                         # tidak dikenal di mana pun
    import master_product as mp
    rep, skus = pr.read_catalogue_input(path, CURRENT, [mp.Product("ZZ-9", "PRODUK ZZ", "5g")])
    assert not rep.ok and any("QQ-0" in e for e in rep.errors)
    assert [(s.product_id, s.product_name, s.isi) for s in skus] == [
        (first.product_id, first.product_name, first.isi), ("ZZ-9", "PRODUK ZZ", "5g")]
    rep2, _ = pr.read_catalogue_input(path, CURRENT, None)
    assert any("--bq" in e for e in rep2.errors)


def test_catalogue_rejects_duplicates_missing_bpom_and_warns_on_odd_bpom_and_removals(tmp_path):
    a = CURRENT[0]
    path = xlsx(tmp_path / "c.xlsx", [
        ["PRODUCT ID", "PRODUCT NAME", "ISI (GR / ML)", "BPOM NUMBER"],
        [a.product_id, a.product_name, a.isi, a.bpom_number], [a.product_id, a.product_name, a.isi, a.bpom_number.lower()],
        [a.product_id, a.product_name, a.isi, None], [a.product_id, a.product_name, a.isi, "BPOM-SALAH"]])
    rep, _ = pr.read_catalogue_input(path, CURRENT)
    assert any("ganda" in e for e in rep.errors) and any("wajib diisi" in e for e in rep.errors)
    assert any("tidak berpola" in w for w in rep.warnings) and any("Hilang dari katalog baru" in w for w in rep.warnings)


def test_real_current_catalogue_roundtrips_without_changes(sandbox):
    """Katalog saat ini dimasukkan lagi -> tidak ada perubahan (+0/-0)."""
    rows = [["PRODUCT ID", "PRODUCT NAME", "ISI (GR / ML)", "BPOM NUMBER"]] + [[s.product_id, s.product_name, s.isi, s.bpom_number] for s in CURRENT]
    path = xlsx(sandbox / "same.xlsx", rows)
    rep, skus = pr.read_catalogue_input(path, CURRENT)
    assert rep.ok and len(skus) == len(CURRENT) and any("+0 baru, -0 hilang, 0 nama/ISI berubah" in i for i in rep.info)
