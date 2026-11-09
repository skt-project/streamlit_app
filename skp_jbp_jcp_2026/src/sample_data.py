"""Fictional sample rows used in mock/local mode when no service account is
configured. Shapes mirror the real 'Template' sheet (junk formula row, '#N/A',
duplicate group rows, region spelling variants, '/' in names) — no real stores."""

SAMPLE_SOURCE_VALUES: list[list[str]] = [
    ["Jenis Program", "Brand", "Kode Toko/Group", "Nama Toko/Group", "Region",
     "SKP Q1 2026", "SKP Q2 2026", "SKP Q3 2026", "SKP Q4 2026"],
    ['"JBP"()', '"SKINTIFIC"()', "", "", "", "", "", "", ""],
    ["JBP", "SKINTIFIC", "DEMO001", "TOKO CONTOH CANTIK", "East Java",
     "", "", "9901/SKT/SKP/P1B/GT/X/2026", "9951/SKT/SKP/P1B/GT/XI/2026"],
    ["JCP", "SKINTIFIC", "DEMO001", "TOKO CONTOH CANTIK", "East Java",
     "", "", "", "9061/SKT/SKP/P2/GT/X/2026"],
    ["JBP", "FACERINNA", "DEMO001", "TOKO CONTOH CANTIK", "East Java",
     "", "", "9280/FCR/SKP/P1B/GT/X/2026", ""],
    ["JBP", "SKINTIFIC", "DEMO GROUP", "Contoh Beauty Group", "EAST JAVA",
     "9574/SKT/SKP/P1B/GT/X/2026", "9606/SKT/SKP/P1B/GT/X/2026", "9384/SKT/SKP/P1B/GT/X/2026", "9816/SKT/SKP/P1B/GT/XI/2026"],
    ["JBP", "SKINTIFIC", "DEMO GROUP", "Contoh Beauty Group", "EAST JAVA",
     "9574/SKT/SKP/P1B/GT/X/2026", "9606/SKT/SKP/P1B/GT/X/2026", "9384/SKT/SKP/P1B/GT/X/2026", "9816/SKT/SKP/P1B/GT/XI/2026"],
    ["JCP", "TIMEPHORIA", "DEMO GROUP", "Contoh Beauty Group", "East Java",
     "", "9120/TPH/SKP/P2/GT/X/2026", "", ""],
    ["JBP", "SKINTIFIC", "DEMO002", "TOKO SATU / TOKO DUA (CONTOH)", "North Central Java",
     "", "", "9381/SKT/SKP/P1B/GT/X/2026", "9812/SKT/SKP/P1B/GT/XI/2026"],
    ["JCP", "FACERINNA", "DEMO003", "Toko Contoh Sejahtera", "Bali Nusa Tenggara",
     "", "", "", "9045/FCR/SKP/P2/GT/X/2026"],
    ["JBP", "#N/A", "DEMO004", "Toko Contoh Tanpa Region", "",
     "", "", "", ""],
    ["JBP", "SKINTIFIC", "DEMO005", "CV. CONTOH MAJU", "West Java (Sd)",
     "", "", "9030/SKT/SKP/P1B/GT/X/2026", "91009/SKT/SKP/P1B/GT/XI/2026"],
]
