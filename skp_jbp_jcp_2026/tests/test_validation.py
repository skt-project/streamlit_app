"""Tests for the acceptance rules in the spec (section 15)."""
import pytest

from src.sheets import (
    LOG_HEADERS,
    LogFormatError,
    SourceFormatError,
    detect_programs,
    expected_documents,
    list_stores,
    logs_to_frame,
    parse_source_values,
    plan_log_upsert,
    region_options,
    search_stores_by_code,
    search_stores_by_name,
    store_from_index,
    stores_in_region,
)
from src.validation import (
    MAX_FILE_BYTES,
    build_object_path,
    build_target_filename,
    clean_cell,
    filename_resembles_skp,
    filename_warning,
    normalize_filename,
    region_folder,
    validate_pdf,
    validate_pic,
)
from tests.conftest import pdf_bytes

SKP = "9280/FCR/SKP/P1B/GT/X/2026"


# --- ukuran file maksimal 1 MB ----------------------------------------------

def test_pdf_exactly_1mb_is_accepted():
    assert validate_pdf("a.pdf", pdf_bytes(MAX_FILE_BYTES)).ok


def test_pdf_over_1mb_is_rejected_with_clear_message():
    check = validate_pdf("a.pdf", pdf_bytes(MAX_FILE_BYTES + 1))
    assert not check.ok
    assert "melebihi batas 1.00 MB" in check.errors[0]


def test_empty_file_is_rejected():
    assert not validate_pdf("a.pdf", b"").ok


# --- hanya PDF ---------------------------------------------------------------

@pytest.mark.parametrize("name", ["scan.jpg", "dokumen.docx", "pdf", "file.pdf.exe"])
def test_non_pdf_extension_is_rejected(name):
    check = validate_pdf(name, pdf_bytes())
    assert not check.ok
    assert "Hanya file PDF" in check.errors[0]


def test_pdf_extension_is_case_insensitive():
    assert validate_pdf("SKP.PDF", pdf_bytes()).ok


def test_renamed_non_pdf_content_is_rejected():
    check = validate_pdf("foto.pdf", b"\xff\xd8\xff\xe0 JPEG data")
    assert not check.ok
    assert "bukan PDF" in check.errors[0]


# --- normalisasi nama file ---------------------------------------------------

def test_target_filename_follows_pattern_and_replaces_slashes():
    name = build_target_filename("CONTOH BEAUTY STORE", SKP, "FACERINNA", "JBP", "2026 Q4")
    assert name == "SKP - CONTOH BEAUTY STORE - 9280_FCR_SKP_P1B_GT_X_2026.pdf"


def test_slash_in_store_name_becomes_underscore():
    name = build_target_filename("TOKO SATU / TOKO DUA", "9381/SKT/SKP/P1B/GT/X/2026", "SKINTIFIC", "JBP", "2026 Q3")
    assert name == "SKP - TOKO SATU _ TOKO DUA - 9381_SKT_SKP_P1B_GT_X_2026.pdf"


def test_illegal_characters_removed_and_pdf_extension_kept():
    assert normalize_filename('Toko: "A*B?" <C>|#[1].PDF') == "Toko AB C1.pdf"
    assert normalize_filename("tanpa ekstensi") == "tanpa ekstensi.pdf"
    assert normalize_filename("  ...  ") == "dokumen.pdf"


def test_nbsp_and_double_spaces_are_collapsed():
    assert normalize_filename("TOKO  ALFA   BARU.pdf") == "TOKO ALFA BARU.pdf"


def test_long_names_are_truncated_but_keep_extension():
    name = normalize_filename("X" * 400)
    assert len(name) <= 180 and name.endswith(".pdf")


def test_missing_skp_number_gives_deterministic_fallback():
    a = build_target_filename("TOKO ALFA", "", "TIMEPHORIA", "JCP", "2026 Q2")
    b = build_target_filename("TOKO ALFA", "  ", "TIMEPHORIA", "JCP", "2026 Q2")
    assert a == b == "SKP - TOKO ALFA - TANPA-NOMOR_TIMEPHORIA_JCP_2026_Q2.pdf"


# --- object path GCS ---------------------------------------------------------

def test_object_path_layout():
    path = build_object_path("2026 Q1", "JBP", "East Java", "SKP - A - 1_X.pdf")
    assert path == "2026 Q1/jbp/East Java/SKP - A - 1_X.pdf"


def test_object_path_for_jcp_and_region_with_parentheses():
    assert build_object_path("2026 Q4", "jcp", "West Java (Sd)", "f.pdf") == "2026 Q4/jcp/West Java (Sd)/f.pdf"


def test_object_path_empty_region_uses_placeholder_folder():
    assert build_object_path("2026 Q2", "JBP", "", "f.pdf") == "2026 Q2/jbp/TANPA REGION/f.pdf"
    assert region_folder("#N/A") == "TANPA REGION"


def test_object_path_is_deterministic():
    args = ("2026 Q3", "JBP", "Bali Nusa Tenggara", build_target_filename("A/B", SKP, "SKINTIFIC", "JBP", "2026 Q3"))
    assert build_object_path(*args) == build_object_path(*args)
    assert "//" not in build_object_path(*args)


@pytest.mark.parametrize("quarter,program", [("2025 Q1", "JBP"), ("Q1", "JBP"), ("2026 Q1", "XYZ")])
def test_object_path_rejects_unknown_quarter_or_program(quarter, program):
    with pytest.raises(ValueError):
        build_object_path(quarter, program, "East Java", "f.pdf")


# --- warning nama file vs nomor SKP -------------------------------------------

@pytest.mark.parametrize("filename", [
    "SKP - CONTOH BEAUTY STORE - 9280_FCR_SKP_P1B_GT_X_2026.pdf",
    "9280-FCR-SKP-P1B-GT-X-2026.pdf",
    "SKP 9280 FCR P1B GT 2026.pdf",
    "skp_09280_fcr_p1b_gt_x_2026.pdf",
])
def test_similar_filenames_do_not_warn(filename):
    assert filename_resembles_skp(filename, SKP)
    assert filename_warning(filename, SKP) is None


@pytest.mark.parametrize("filename", [
    "scan_001.pdf",
    "SKP - CONTOH BEAUTY STORE - 9281_FCR_SKP_P1B_GT_X_2026.pdf",
    "dokumen.pdf",
    "SKP 9280.pdf",
])
def test_dissimilar_filenames_warn_but_are_not_rejected(filename):
    assert not filename_resembles_skp(filename, SKP)
    assert "tidak mirip" in filename_warning(filename, SKP)
    assert validate_pdf(filename, pdf_bytes()).ok  # warning only, never blocking


def test_no_skp_number_gives_informational_warning():
    assert "tidak tersedia" in filename_warning("apa saja.pdf", "")


# --- pencarian toko ------------------------------------------------------------

def test_search_by_code_exact_first_and_case_insensitive(index):
    results = search_stores_by_code(index, "t100")
    assert [s.code for s in results] == ["T100"]
    assert results[0].name == "TOKO ALFA" and results[0].region == "East Java"


def test_search_by_code_ignores_spaces_and_punctuation(index):
    assert [s.code for s in search_stores_by_code(index, "beta-group")] == ["BETA GROUP"]


def test_search_by_code_partial_match_ranks_prefix_before_contains(index):
    codes = [s.code for s in search_stores_by_code(index, "D30")]
    assert codes == ["D300", "D301"]


def test_search_by_name_partial_case_insensitive(index):
    assert [s.code for s in search_stores_by_name(index, "alfa")] == ["T100"]


def test_search_by_name_returns_every_code_sharing_a_name(index):
    assert {s.code for s in search_stores_by_name(index, "DELTA BEAUTY")} == {"D300", "D301"}


def test_search_by_name_matches_name_variants(index):
    results = search_stores_by_name(index, "CV. GAMMA")
    assert [s.code for s in results] == ["G200"]
    assert results[0].name == "CV.GAMMA JAYA"  # most frequent variant is canonical
    assert results[0].aliases == ("CV. GAMMA JAYA",)


def test_search_with_blank_query_returns_nothing(index):
    assert search_stores_by_code(index, "  ") == []
    assert search_stores_by_name(index, "") == []


def test_store_from_index_unknown_code(index):
    assert store_from_index(index, "NOPE") is None


# --- parsing sheet sumber --------------------------------------------------------

def test_parse_drops_junk_rows_duplicates_and_error_markers(source_df):
    assert "" not in set(source_df["Kode Toko/Group"])
    assert len(source_df[source_df["Kode Toko/Group"] == "BETA GROUP"]) == 1
    e400 = source_df[source_df["Kode Toko/Group"] == "E400"].iloc[0]
    assert e400["Brand"] == "" and e400["Region"] == ""


def test_parse_unifies_region_spelling(source_df):
    regions = set(source_df[source_df["Kode Toko/Group"].isin(["T100", "BETA GROUP"])]["Region"])
    assert regions == {"East Java"}


def test_parse_tolerates_extra_and_reordered_columns():
    values = [["Catatan", "Brand", "Jenis Program", "Nama Toko/Group", "Kode Toko/Group", "Region",
               "SKP Q4 2026", "SKP Q3 2026", "SKP Q2 2026", "SKP Q1 2026"],
              ["x", "SKINTIFIC", "JBP", "TOKO Z", "Z1", "Papua Maluku", "9/SKT", "", "", ""]]
    df = parse_source_values(values)
    assert df.iloc[0]["SKP Q4 2026"] == "9/SKT"
    assert df.iloc[0]["Kode Toko/Group"] == "Z1"


def test_parse_missing_columns_raises_clear_error():
    with pytest.raises(SourceFormatError, match="SKP Q4 2026"):
        parse_source_values([["Jenis Program", "Brand", "Kode Toko/Group", "Nama Toko/Group", "Region"]])


def test_clean_cell_maps_formula_errors_to_blank():
    assert clean_cell("#N/A") == "" and clean_cell("  A  B ") == "A B"


# --- auto-detection brand/program/kuartal ----------------------------------------------

def test_detect_brand_program_and_quarters(source_df):
    det = detect_programs(source_df, "T100")
    assert det.brands == {"SKINTIFIC", "FACERINNA"}
    assert det.programs == {("SKINTIFIC", "JBP"), ("SKINTIFIC", "JCP"), ("FACERINNA", "JBP")}
    assert det.quarters_for("SKINTIFIC", "JBP") == ["2026 Q3", "2026 Q4"]
    assert det.quarters_for("SKINTIFIC", "JCP") == ["2026 Q4"]
    assert det.quarters_for("FACERINNA", "JBP") == ["2026 Q1"]
    assert det.skp_number("SKINTIFIC", "JBP", "2026 Q4") == "9784/SKT/SKP/P1B/GT/XI/2026"
    assert det.skp_number("SKINTIFIC", "JBP", "2026 Q1") == ""


def test_detect_is_case_insensitive_on_code_and_merges_group_rows(source_df):
    det = detect_programs(source_df, "beta group")
    assert det.programs == {("SKINTIFIC", "JBP")}
    assert det.quarters_for("SKINTIFIC", "JBP") == ["2026 Q1"]
    assert not det.conflicts


def test_detect_ignores_unknown_brand(source_df):
    det = detect_programs(source_df, "E400")
    assert det.brands == set() and det.programs == set()


def test_detect_reports_conflicting_skp_numbers():
    df = parse_source_values([
        ["Jenis Program", "Brand", "Kode Toko/Group", "Nama Toko/Group", "Region",
         "SKP Q1 2026", "SKP Q2 2026", "SKP Q3 2026", "SKP Q4 2026"],
        ["JBP", "SKINTIFIC", "C1", "A", "R", "1/SKT", "", "", ""],
        ["JBP", "SKINTIFIC", "C1", "B", "R", "2/SKT", "", "", ""],
    ])
    det = detect_programs(df, "C1")
    assert det.skp_number("SKINTIFIC", "JBP", "2026 Q1") == "1/SKT"
    assert det.conflicts


def test_expected_documents_one_row_per_combination(source_df):
    exp = expected_documents(source_df)
    beta = exp[exp["Kode Toko/Group"] == "BETA GROUP"]
    assert len(beta) == 1 and beta.iloc[0]["Kuartal"] == "2026 Q1"
    assert len(exp[exp["Kode Toko/Group"] == "G200"]) == 2


# --- overwrite metadata (toko + brand + program + kuartal) ------------------------------------

def _record(code="T100", brand="SKINTIFIC", program="JBP", quarter="2026 Q4", pic="Ana", path="p1"):
    return {"Timestamp": "2026-11-09 10:00:00", "PIC Submit": pic, "Kode Toko/Group": code,
            "Nama Toko/Group": "TOKO ALFA", "Region": "East Java", "Brand": brand, "Jenis Program": program,
            "Kuartal": quarter, "Nomor SKP": "9784/SKT", "Nama File": "f.pdf", "GCS Path": path, "Uploader": pic}


def _row(rec, status="Uploaded"):
    return [rec.get(h, status if h == "Status" else "") for h in LOG_HEADERS]


def test_first_upload_writes_header_and_appends():
    plan = plan_log_upsert([], [_record()])
    assert plan.write_header and plan.header == LOG_HEADERS
    assert plan.updates == [] and len(plan.appends) == 1
    assert plan.appends[0][LOG_HEADERS.index("Status")] == "Uploaded"


def test_same_combination_is_updated_not_duplicated():
    existing = [LOG_HEADERS, _row(_record(pic="Ana"))]
    plan = plan_log_upsert(existing, [_record(pic="Budi", path="p2")])
    assert plan.appends == []
    assert len(plan.updates) == 1
    row_number, row = plan.updates[0]
    assert row_number == 2
    assert row[LOG_HEADERS.index("PIC Submit")] == "Budi"
    assert row[LOG_HEADERS.index("Status")] == "Replaced"
    key = ("T100", "SKINTIFIC", "JBP", "2026 Q4")
    assert plan.previous[key]["GCS Path"] == "p1"
    assert plan.paths_in_use == {"p2"}


def test_key_match_is_case_and_whitespace_insensitive():
    existing = [LOG_HEADERS, [_record(code="beta group ")[h] if h in _record() else "" for h in LOG_HEADERS]]
    plan = plan_log_upsert(existing, [_record(code="BETA GROUP")])
    assert len(plan.updates) == 1 and not plan.appends


@pytest.mark.parametrize("field,value", [("Brand", "FACERINNA"), ("Jenis Program", "JCP"),
                                         ("Kuartal", "2026 Q3"), ("Kode Toko/Group", "G200")])
def test_different_key_part_creates_new_row(field, value):
    existing = [LOG_HEADERS, [_record()[h] if h in _record() else "" for h in LOG_HEADERS]]
    rec = _record()
    rec[field] = value
    plan = plan_log_upsert(existing, [rec])
    assert not plan.updates and len(plan.appends) == 1


def test_duplicate_keys_inside_one_batch_last_wins():
    plan = plan_log_upsert([], [_record(pic="A"), _record(pic="B")])
    assert len(plan.appends) == 1
    assert plan.appends[0][LOG_HEADERS.index("PIC Submit")] == "B"


def test_extra_manual_columns_are_preserved_on_update():
    header = LOG_HEADERS + ["Catatan Admin"]
    existing = [header, [_record()[h] if h in _record() else "" for h in LOG_HEADERS] + ["sudah dicek"]]
    plan = plan_log_upsert(existing, [_record(pic="Budi")])
    assert plan.updates[0][1][-1] == "sudah dicek"


def test_log_header_missing_column_raises():
    with pytest.raises(LogFormatError, match="GCS Path"):
        plan_log_upsert([[h for h in LOG_HEADERS if h != "GCS Path"]], [_record()])


def test_name_check_note_goes_into_status():
    rec = {**_record(), "_note": "cek nama file"}
    plan = plan_log_upsert([], [rec])
    assert plan.appends[0][LOG_HEADERS.index("Status")] == "Uploaded (cek nama file)"
    assert "_note" not in plan.header


def test_logs_to_frame_dedupes_accidental_duplicates_latest_wins():
    old = [_record(pic="Lama")[h] if h in _record() else "" for h in LOG_HEADERS]
    new = list(old)
    new[LOG_HEADERS.index("Timestamp")] = "2026-11-10 08:00:00"
    new[LOG_HEADERS.index("PIC Submit")] = "Baru"
    df = logs_to_frame([LOG_HEADERS, new, old])
    assert len(df) == 1 and df.iloc[0]["PIC Submit"] == "Baru"


# --- PIC ----------------------------------------------------------------------------

def test_pic_is_required_and_bounded():
    assert validate_pic("  ") == "PIC Submit wajib diisi."
    assert validate_pic("A" * 81) is not None
    assert validate_pic("Budi Santoso") is None


# --- region dulu, baru daftar toko --------------------------------------------------

def test_region_options_sorted_with_counts_and_blank_region_last(index):
    assert region_options(index) == [("Bali Nusa Tenggara", 1), ("East Java", 2), ("Papua Maluku", 2), ("", 1)]


def test_stores_in_region_and_list_sorted_by_name(index):
    stores = list_stores(stores_in_region(index, "East Java"))
    assert [s.code for s in stores] == ["BETA GROUP", "T100"]  # "Beta Beauty" < "TOKO ALFA"
    assert stores[0].short_label == "BETA GROUP — Beta Beauty"


def test_blank_region_stores_are_reachable(index):
    assert [s.code for s in list_stores(stores_in_region(index, ""))] == ["E400"]


def test_search_inside_region_only(index):
    papua = stores_in_region(index, "Papua Maluku")
    assert {s.code for s in search_stores_by_name(papua, "delta")} == {"D300", "D301"}
    assert search_stores_by_code(papua, "T100") == []
