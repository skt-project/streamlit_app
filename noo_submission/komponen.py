import pandas as pd
from html import escape

def _satu_baris(teks: str) -> str:
    """Collapse multi-line HTML string menjadi satu baris (buang newline & spasi berlebih)."""
    import re
    return re.sub(r"\s+", " ", teks).strip()
STATUS_VALID = "✅ Valid"
STATUS_REVIEW = "⚠️ Need Review"


# =========================== PAGE HEADER ===========================
def html_header(judul: str, subjudul: str, ikon: str = "🏪") -> str:
    return (
        f'<div class="header-halaman"><div class="header-kiri"><div class="header-ikon">{ikon}</div><div>'
        f'<div class="header-judul">{judul}</div><div class="header-sub">{subjudul}</div></div></div>'
        f'<div class="pengguna-box"><span class="avatar-bulat">👤</span></div></div>'
    )

# =========================== NOO NOTES BANNER ===========================
def html_catatan_noo() -> str:
    return (
        '<div class="catatan-noo-bar">'
        '<b>Complete each outlet\'s data before submitting</b>'
        '<span>Store Address is automatically taken from the file</span>'
        '</div>'
    )
# =========================== BREADCRUMB ===========================
def html_breadcrumb(label_halaman: str) -> str:
    return f'<div class="breadcrumb-nav">Home / <b>{label_halaman}</b></div>'


# =========================== STORE VALIDATION KPI CARDS ===========================
def html_kpi_validasi(df) -> str:
    total = len(df)
    status = df["Validasi"].astype(str).str.strip() if total and "Validasi" in df.columns else None

    def hitung(nama: str) -> int:
        return int((status == nama).sum()) if status is not None else 0

    def persen(n: int) -> int:
        return round(n * 100 / total) if total else 0

    n_generate, n_review, n_existing = hitung("Generate ID"), hitung("Need Review"), hitung("Existing")

    def kartu(kelas: str, ikon: str, label: str, nilai: int, badge: bool = True) -> str:
        small = f"<small>({persen(nilai)}%)</small>" if badge else ""
        return (f'<div class="kartu-kpi {kelas}"><div class="kpi-ikon">{ikon}</div>'
                f'<div><div class="kpi-label">{label}</div>'
                f'<div class="kpi-nilai">{nilai:,} {small}</div></div></div>')

    return ('<div class="grid-kpi">'
            + kartu("", "🏪", "Total Stores", total, badge=False)
            + kartu("hijau", "✓", "Generate ID (Success)", n_generate)
            + kartu("merah", "⚠️", "Need Review", n_review)
            + kartu("kuning", "📌", "Existing", n_existing)
            + "</div>")
 


# =========================== UPLOADED FILE INFO ===========================
def html_info_file(nama_file: str, ukuran: str) -> str:
    return (
        '<div class="info-file"><div class="info-file-ikon">📄</div>'
        f'<div><b>{nama_file}</b><span>{ukuran} · Ready to process ✓</span></div></div>'
    )


# =========================== UPLOAD ERROR MESSAGE ===========================
def html_error_unggah(pesan: str) -> str:
    return f'<div class="pesan-error-unggah">⚠️ {pesan}</div>'


# =========================== SUBMISSION CARD HEADER ===========================
def html_header_pengajuan(ikon: str, judul: str, deskripsi: str) -> str:
    return (
        '<div class="pengajuan-header">'
        f'<div class="pengajuan-ikon">{ikon}</div>'
        f'<div><h3>{judul}</h3><p>{deskripsi}</p></div></div>'
    )


# =========================== PREVIEW SECTION TITLE ===========================
def html_judul_preview(jumlah_data: int | None) -> str:
    kanan = f"<b>{jumlah_data} records found</b>" if jumlah_data is not None else ""
    return f'<div class="judul-bagian"><span>Data Preview</span>{kanan}</div>'


# =========================== PREVIEW TABLE (SKU / NOO) ===========================
def html_tabel_preview(header_kolom: list[str], baris_data: list[list[str]]) -> str:
    if not baris_data:
        return '<div class="preview-kosong">📄 No data yet, please upload a file first</div>'
    head = "".join(f"<th>{h}</th>" for h in header_kolom)
    body = "".join(
        "<tr>" + "".join(f"<td>{nilai}</td>" for nilai in baris) + "</tr>" for baris in baris_data
    )
    return f'<table class="tabel-preview"><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>'


# =========================== STATUS BADGE (used in SPV table) ===========================

_BADGE = {
    "valid":   ("badge-valid",   "Valid"),
    "invalid": ("badge-invalid", "Kode kosong"),
    "review":  ("badge-review",  "Review"),
    "none":    ("badge-none",    "-"),
}

def html_badge_status(status: str) -> str:
    kelas, teks = _BADGE.get(status, _BADGE["none"])
    return f'<span class="badge-status {kelas}">{teks}</span>'


# ----------------------------------------------------------------------
# Konfirmasi SPV (tabel outlet)
# ----------------------------------------------------------------------
def html_kepala_kolom(teks: str, wajib: bool = False) -> str:
    bintang = ' <span class="bintang">*</span>' if wajib else ""
    return f'<div class="kepala-kolom">{escape(teks)}{bintang}</div>'


def html_sel_teks(teks, gaya: str = "") -> str:
    """Sel teks read-only. gaya: '' | 'sel-baca' (abu, data tarikan) | 'sel-tebal'."""
    isi = escape(str(teks)) if str(teks).strip() else "-"
    return f'<div class="sel-teks {gaya}">{isi}</div>'


def html_pesan_error(teks: str) -> str:
    return f'<div class="pesan-error-kecil">⚠ {escape(teks)}</div>'


def html_catatan_spv() -> str:
    return _satu_baris("""
        <div class="catatan-noo">
            Pilih <b>NOO</b> atau <b>Existing</b> untuk tiap outlet.
            Jika <b>Existing</b>, kolom <b>Store ID (Validation)</b> wajib diisi.
            Kolom abu-abu diambil dari data dan tidak bisa diedit.
        </div>
    """)


def html_ringkasan_spv(total: int, selesai: int, noo: int, existing: int,
                       belum: int, kurang_id: int) -> str:
    persen = (selesai / total * 100) if total else 0
    return (
        '<div class="ringkasan-spv">'
        '<div class="ringkasan-atas">'
        f'<span class="ringkasan-judul">{selesai:,} dari {total:,} outlet sudah lengkap</span>'
        f'<span class="ringkasan-persen">{persen:.0f}%</span>'
        '</div>'
        f'<div class="bar-luar"><div class="bar-isi" style="width:{persen:.1f}%"></div></div>'
        '<div class="chip-baris">'
        f'<span class="chip chip-noo">NOO <b>{noo:,}</b></span>'
        f'<span class="chip chip-existing">Existing <b>{existing:,}</b></span>'
        f'<span class="chip chip-belum">Belum dipilih <b>{belum:,}</b></span>'
        f'<span class="chip chip-kurang{" chip-aktif" if kurang_id else ""}">Store ID kosong <b>{kurang_id:,}</b></span>'
        '</div>'
        '</div>'
    )


def html_info_region(region) -> str:
    """Chip kecil: region distributor yang dipakai untuk skor similarity."""
    if region:
        isi = escape(", ".join(region))
        return f'<div class="info-region">Region: <b>{isi}</b></div>'
    return '<div class="info-region info-region-kosong">Region: - <span>(pilih distributor)</span></div>'
