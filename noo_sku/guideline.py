"""Indonesian user guideline — shown in the app and downloadable as PDF.

Single source of truth: the UI expander and the PDF are rendered from the same
structure, so they cannot drift apart. Everything described here is behaviour
the application actually implements — nothing aspirational.
"""
from __future__ import annotations

from io import BytesIO

TITLE = "Panduan Penginputan NOO & SKU Mapping"

SECTIONS = [
    ("1. Login", [
        "Login menggunakan Kode Distributor (DSTxxx) dan password Anda.",
        "Kode Distributor hasil login menjadi identitas resmi Anda di seluruh "
        "proses upload.",
        "Semua data yang Anda upload otomatis terikat ke Kode Distributor ini.",
    ]),
    ("2. Identitas Distributor", [
        "Nama distributor, region, ASM, dan kode singkatan diambil otomatis "
        "dari data master.",
        "Anda TIDAK perlu mengisi informasi distributor secara manual.",
        "Jika file berisi Kode Distributor yang berbeda dengan akun login, "
        "upload akan DITOLAK. Perbaiki file atau login dengan akun yang sesuai.",
    ]),
    ("3. Download Template", [
        "Gunakan tombol Download Template pada masing-masing section.",
        "Selalu gunakan template terbaru dari aplikasi ini.",
    ]),
    ("4. Mengisi Template", [
        "Jangan mengubah nama kolom (header).",
        "Jangan menambah, menghapus, atau memindahkan kolom.",
        "Jangan menghapus sheet apa pun di dalam file template.",
        "Baris CONTOH tidak perlu dihapus — sistem otomatis mengabaikannya.",
        "Isi data mulai dari baris di bawah baris CONTOH.",
    ]),
    ("5. Kolom yang Anda Isi (NOO)", [
        "Store ID — OPSIONAL, boleh dikosongkan.",
        "Store Name — wajib.",
        "Channel — wajib, GT atau MTI.",
        "Customer Code — wajib, kode brand (11/13/1A) + singkatan distributor.",
        "Customer Store Code — wajib, diawali kode distributor Anda "
        "(contoh: DST12300010).",
        "City — wajib.",
        "Store Address — wajib, selengkap mungkin.",
        "Store Type — wajib, sesuai channel yang dipilih.",
    ]),
    ("6. Kolom yang Anda Isi (SKU)", [
        "Principal Product Code — wajib, harus ada di master produk prinsipal.",
        "Principal Product Name — wajib.",
        "Product Size (ml/g) — wajib, mengikuti ketentuan prinsipal.",
        "Customer Product Code — wajib, kode produk milik distributor.",
        "Customer Product Name — wajib, nama produk milik distributor.",
    ]),
    ("7. Kolom yang Diisi Otomatis oleh Sistem", [
        "input_time — waktu upload, zona waktu Asia/Jakarta (WIB).",
        "Nama distributor / branch name, region, dan ASM.",
        "Customer Code untuk SKU Mapping (kode brand + singkatan distributor).",
        "Nama dan spesifikasi produk diambil dari master produk prinsipal.",
        "SE, SPV, AOM, Area, dan Province diambil dari master toko bila "
        "tersedia.",
    ]),
    ("8. Toko Baru (NOO) — Kolom Enrichment Kosong", [
        "Untuk toko yang benar-benar baru, data seperti SE, SPV, AOM, Area, "
        "dan Province belum ada di master.",
        "Hal ini WAJAR dan TIDAK menyebabkan upload gagal.",
        "Kolom tersebut akan dikosongkan dan dilengkapi oleh BD Support.",
        "Yang menyebabkan upload gagal hanyalah kolom input wajib yang kosong "
        "atau tidak valid.",
    ]),
    ("9. Pengecekan Duplikat", [
        "Pengecekan dilakukan PER BARIS, bukan per file.",
        "Baris yang isinya persis sama dengan data yang sudah pernah diupload "
        "akan dilewati (tidak dimasukkan lagi).",
        "Baris duplikat TIDAK membatalkan baris lain yang masih valid.",
    ]),
    ("10. Koreksi Data", [
        "Jika toko/produk sudah pernah diupload tetapi ada isi yang berubah, "
        "baris tersebut dianggap KOREKSI.",
        "Baris koreksi tetap dimasukkan sebagai BARIS BARU.",
        "Data lama tidak diubah atau dihapus. BD Support yang menentukan data "
        "mana yang dipakai.",
    ]),
    ("11. Duplikat di Dalam Satu File", [
        "Jika satu baris muncul lebih dari sekali di file yang sama, sistem "
        "memberi peringatan.",
        "Hanya baris pertama yang diproses. Mohon periksa kembali file Anda.",
    ]),
    ("12. Error dan Perbaikan", [
        "Error ditampilkan per baris, lengkap dengan nomor baris, nama kolom, "
        "masalah, dan saran perbaikan.",
        "Laporan error bisa diunduh dalam format Excel.",
        "Perbaiki file lalu upload ulang.",
    ]),
    ("13. Konfirmasi Sebelum Upload", [
        "Sebelum data masuk, sistem menampilkan ringkasan: jumlah baris baru, "
        "koreksi, duplikat, dan error.",
        "Data BARU DITULIS setelah Anda menekan tombol konfirmasi.",
        "Selama belum dikonfirmasi, tidak ada data yang masuk ke tracker.",
    ]),
    ("14. Setelah Upload", [
        "Data masuk ke pool tracker dan diperiksa BD Support setiap hari.",
        "Untuk SKU Mapping, mohon konfirmasi ke BD Support "
        "(Intan / Surti) setelah upload.",
    ]),
]


def as_markdown() -> str:
    parts = []
    for heading, items in SECTIONS:
        parts.append(f"**{heading}**")
        parts.extend(f"- {item}" for item in items)
        parts.append("")
    return "\n".join(parts)


def build_pdf() -> bytes:
    """Render the same content as a PDF. Returns empty bytes if unavailable."""
    try:
        from reportlab.lib.enums import TA_LEFT
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import mm
        from reportlab.platypus import (ListFlowable, ListItem, Paragraph,
                                        SimpleDocTemplate, Spacer)
    except ImportError:
        return b""

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4, leftMargin=18 * mm, rightMargin=18 * mm,
        topMargin=18 * mm, bottomMargin=18 * mm, title=TITLE)
    styles = getSampleStyleSheet()
    h1 = ParagraphStyle("h1", parent=styles["Title"], fontSize=16, spaceAfter=10)
    h2 = ParagraphStyle("h2", parent=styles["Heading2"], fontSize=11,
                        spaceBefore=8, spaceAfter=4, alignment=TA_LEFT)
    body = ParagraphStyle("body", parent=styles["BodyText"], fontSize=9,
                          leading=13)

    story = [Paragraph(TITLE, h1),
             Paragraph("Panduan untuk Admin Distributor — dokumen ini "
                       "mengikuti perilaku aplikasi yang berjalan.", body),
             Spacer(1, 6)]
    for heading, items in SECTIONS:
        story.append(Paragraph(heading, h2))
        story.append(ListFlowable(
            [ListItem(Paragraph(item, body), leftIndent=10) for item in items],
            bulletType="bullet", start="•", leftIndent=12))
    doc.build(story)
    return buf.getvalue()
