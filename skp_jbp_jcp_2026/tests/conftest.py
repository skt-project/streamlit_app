import pytest

from src.sheets import Store, build_store_index, parse_source_values

HEADER = ["Jenis Program", "Brand", "Kode Toko/Group", "Nama Toko/Group", "Region",
          "SKP Q1 2026", "SKP Q2 2026", "SKP Q3 2026", "SKP Q4 2026"]

# Fictional rows that reproduce the quirks of the real sheet.
ROWS = [
    HEADER,
    ['"JBP"()', '"SKINTIFIC"()', "", "", "", "", "", "", ""],
    ["JBP", "SKINTIFIC", "T100", "TOKO ALFA", "East Java", "", "", "9004/SKT/SKP/P1B/GT/X/2026", "9784/SKT/SKP/P1B/GT/XI/2026"],
    ["JCP", "SKINTIFIC", "T100", "TOKO ALFA", "East Java", "", "", "", "9061/SKT/SKP/P2/GT/X/2026"],
    ["JBP", "FACERINNA", "T100", "TOKO ALFA", "east java", "9280/FCR/SKP/P1B/GT/X/2026", "", "", ""],
    ["JBP", "SKINTIFIC", "BETA GROUP", "Beta Beauty", "EAST JAVA", "9574/SKT/SKP/P1B/GT/X/2026", "", "", ""],
    ["JBP", "SKINTIFIC", "BETA GROUP", "Beta Beauty", "EAST JAVA", "9574/SKT/SKP/P1B/GT/X/2026", "", "", ""],
    ["JBP", "SKINTIFIC", "G200", "CV.GAMMA JAYA", "Bali Nusa Tenggara", "", "", "", "9100/SKT/SKP/P1B/GT/XI/2026"],
    ["JCP", "TIMEPHORIA", "G200", "CV. GAMMA JAYA", "Bali Nusa Tenggara", "", "", "", "9200/TPH/SKP/P2/GT/X/2026"],
    ["JBP", "SKINTIFIC", "G200", "CV.GAMMA JAYA", "Bali Nusa Tenggara", "", "", "", "9100/SKT/SKP/P1B/GT/XI/2026"],
    ["JBP", "SKINTIFIC", "D300", "DELTA BEAUTY", "Papua Maluku", "", "", "", "9300/SKT/SKP/P1B/GT/XI/2026"],
    ["JBP", "SKINTIFIC", "D301", "DELTA BEAUTY", "Papua Maluku", "", "", "", "9301/SKT/SKP/P1B/GT/XI/2026"],
    ["JBP", "#N/A", "E400", "TOKO SATU / TOKO DUA", "#N/A", "", "", "", ""],
]


@pytest.fixture
def source_df():
    return parse_source_values([list(r) for r in ROWS])


@pytest.fixture
def index(source_df):
    return build_store_index(source_df)


@pytest.fixture
def store():
    return Store(code="T100", name="TOKO ALFA", region="East Java")


def pdf_bytes(size: int = 2000) -> bytes:
    head = b"%PDF-1.4\n"
    tail = b"\n%%EOF"
    return head + b"0" * max(0, size - len(head) - len(tail)) + tail
