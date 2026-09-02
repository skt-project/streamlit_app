"""Indonesian user guideline — context-sensitive to the selected function.

Three blocks: a deliberately short GENERAL section that genuinely applies to
both functions, then NOO-only and SKU-only content. The app renders GENERAL plus
exactly one of the two, so a distributor working on SKU Mapping never reads NOO
rules and vice versa.

The on-screen expander and the downloadable PDF are rendered from this same
structure, so they cannot drift apart, and each PDF covers only its own function.
"""
from __future__ import annotations

from io import BytesIO

TITLE_NOO = "Panduan Penginputan — NOO / Store Mapping"
TITLE_SKU = "Panduan Penginputan — SKU Mapping"

#: Applies to both functions. Kept short on purpose.
GENERAL = [
    ("Umum", [
        "Login menggunakan Kode Distributor (DSTxxx) dan password Anda.",
        "Akun Anda menentukan **perusahaan** yang boleh Anda input. Anda tidak "
        "dapat mengirim data milik perusahaan lain.",
        "Jangan mengubah nama kolom, urutan kolom, atau menghapus sheet di "
        "dalam file template.",
        "Baris CONTOH tidak perlu dihapus — sistem otomatis mengabaikannya.",
        "Sistem menampilkan ringkasan sebelum data masuk. Data baru ditulis "
        "setelah Anda menekan tombol konfirmasi.",
    ]),
]

NOO = [
    ("Tujuan", [
        "NOO / Store Mapping dipakai untuk mendaftarkan toko baru (new outlet "
        "opening) milik distributor Anda ke tracker.",
    ]),
    ("Kolom yang Anda isi", [
        "**Store ID** — OPSIONAL. Boleh dikosongkan. Kolom ini terisi jika toko "
        "sudah pernah ter-mapping; Anda dapat meminta konfirmasi SPV SKINTIFIC.",
        "**Store Name** — wajib, nama toko lengkap dan benar.",
        "**Channel** — wajib, GT atau MTI.",
        "**Customer Code** — wajib, kode brand (11/13/1A) + singkatan "
        "distributor Anda.",
        "**Customer Store Code** — wajib, gabungan kode distributor + customer "
        "ID toko (contoh: DST12300010). Jangan mengisi customer ID saja.",
        "**City** — wajib. Ikuti ejaan pada sheet *City & Store Type*.",
        "**Store Address** — wajib, selengkap mungkin: jalan, nomor, "
        "kelurahan, kecamatan, kota, kode pos.",
        "**Store Type** — wajib, harus sesuai channel yang dipilih.",
    ]),
    ("Aturan mapping", [
        "Jika satu toko dengan Customer Store Code yang sama belum terdaftar "
        "di lebih dari satu brand, cukup **diinput satu kali**.",
        "**Satu file boleh berisi beberapa cabang.** Isi **Customer Branch "
        "Code** pada setiap baris dengan kode cabang (DSTxxx) yang sesuai "
        "untuk toko tersebut.",
        "Seluruh cabang yang Anda input harus berada di bawah **perusahaan "
        "yang sama** dengan akun Anda. Baris dengan kode cabang di luar "
        "perusahaan Anda akan ditolak dan upload dibatalkan.",
        "**Customer Store Code** harus diawali kode cabang pada baris yang "
        "sama, bukan kode cabang lain.",
    ]),
    ("Yang diproses sistem setelah Anda upload", [
        "Nama Perusahaan dan Kode Cabang pada tracker mengikuti data akun Anda "
        "untuk cabang yang bersangkutan — bukan sekadar apa yang Anda ketik.",
        "Sistem memeriksa apakah toko yang Anda input sudah pernah terdaftar "
        "sebelumnya. Anda tidak perlu melakukan pengecekan ini secara manual.",
    ]),
    ("Validasi & hasil", [
        "Error ditampilkan per baris: nomor baris, kolom, masalah, dan saran "
        "perbaikan. Laporan error bisa diunduh dalam format Excel.",
        "Baris yang isinya persis sama dengan data yang sudah pernah diupload "
        "akan dilewati; baris lain yang valid tetap diproses.",
        "Jika toko sudah pernah diupload tetapi ada isi yang berubah, baris "
        "tersebut dianggap KOREKSI dan tetap dimasukkan sebagai baris baru. "
        "Data lama tidak diubah.",
        "Setelah upload, mohon konfirmasi ke BD Support masing-masing "
        "distributor (Intan / Surti).",
    ]),
]

SKU = [
    ("Tujuan", [
        "SKU Mapping dipakai untuk memetakan kode produk prinsipal ke kode "
        "produk milik distributor Anda.",
    ]),
    ("Kolom yang Anda isi", [
        "**Principal Product Code** — wajib, **harus terdaftar di master produk prinsipal**. Kode yang tidak ditemukan akan menggagalkan upload.",
        "**Principal Product Name** — wajib, nama produk prinsipal.",
        "**Customer Product Code** — wajib, kode produk milik distributor.",
        "**Customer Product Name** — wajib, nama produk milik distributor.",
    ]),
    ("Aturan mapping", [
        "Mapping ini hanya untuk brand **SKINTIFIC, TIMEPHORIA, dan "
        "FACERINNA**. Produk brand lain akan ditolak.",
        "Nama produk mengikuti master prinsipal. Jika isian Anda berbeda, "
        "sistem memberi peringatan dan memakai data master.",
        "Ukuran / gramasi produk **tidak perlu diisi** — kolom tersebut sudah dihapus dari template.",
    ]),
    ("Validasi & hasil", [
        "Kode produk yang tidak ditemukan di master prinsipal akan ditolak "
        "beserta nomor barisnya.",
        "Mapping yang persis sama dengan yang sudah pernah diupload akan "
        "dilewati; baris lain yang valid tetap diproses.",
        "Jika mapping sudah ada tetapi isinya berubah, baris tersebut dianggap "
        "KOREKSI dan dimasukkan sebagai baris baru.",
        "Setelah upload, mohon konfirmasi ke BD Support masing-masing "
        "distributor (Intan / Surti).",
    ]),
]

UPLOAD_NOO = "NOO"
UPLOAD_SKU = "SKU"


def sections_for(kind: str):
    """GENERAL plus exactly one function's sections — never both."""
    specific = NOO if str(kind).upper().startswith("NOO") else SKU
    return GENERAL + specific


def title_for(kind: str) -> str:
    return TITLE_NOO if str(kind).upper().startswith("NOO") else TITLE_SKU


def as_markdown(kind: str) -> str:
    parts = []
    for heading, items in sections_for(kind):
        parts.append(f"**{heading}**")
        parts.extend(f"- {item}" for item in items)
        parts.append("")
    return "\n".join(parts)


def build_pdf(kind: str) -> bytes:
    """PDF covering only the selected function. Empty bytes if unavailable."""
    try:
        from reportlab.lib.enums import TA_LEFT
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import mm
        from reportlab.platypus import (ListFlowable, ListItem, Paragraph,
                                        SimpleDocTemplate, Spacer)
    except ImportError:
        return b""

    import re

    title = title_for(kind)
    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4, leftMargin=18 * mm, rightMargin=18 * mm,
        topMargin=18 * mm, bottomMargin=18 * mm, title=title)
    styles = getSampleStyleSheet()
    h1 = ParagraphStyle("h1", parent=styles["Title"], fontSize=15, spaceAfter=10)
    h2 = ParagraphStyle("h2", parent=styles["Heading2"], fontSize=11,
                        spaceBefore=8, spaceAfter=4, alignment=TA_LEFT)
    body = ParagraphStyle("body", parent=styles["BodyText"], fontSize=9,
                          leading=13)

    def rich(text):
        # Markdown bold -> reportlab markup; the source is ours, not user input.
        return re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)

    story = [Paragraph(title, h1),
             Paragraph("Panduan untuk Admin Distributor — dokumen ini "
                       "mengikuti perilaku aplikasi yang berjalan.", body),
             Spacer(1, 6)]
    for heading, items in sections_for(kind):
        story.append(Paragraph(heading, h2))
        story.append(ListFlowable(
            [ListItem(Paragraph(rich(i), body), leftIndent=10) for i in items],
            bulletType="bullet", start="•", leftIndent=12))
    doc.build(story)
    return buf.getvalue()
