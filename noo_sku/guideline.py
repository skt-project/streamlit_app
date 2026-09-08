"""Indonesian user guideline — context-sensitive to the selected function.

Single source of truth: "Panduan Penggunaan Streamlit untuk NOO & SKU
MAPPING.pptx" (bundled alongside this module, see GUIDE_PPTX_PATH), BD
Support's own 20-slide walkthrough, current as of 2026-09-07 — its own
screenshots already show the REVISI templates (Store ID absent from NOO,
Nama Produk Prinsipal absent from SKU), confirming it was built against this
exact revision of the app. Every step below reproduces that deck's own text
and screenshots; nothing here is invented. One screenshot was deliberately
NOT reproduced — the old NOO step 6 example, which showed a now-removed
Store ID validation error — see the comment at that step below; its
instruction text is kept verbatim, only that one image is omitted.

Three blocks: GENERAL (applies to both), then NOO-only and SKU-only content.
The app renders GENERAL plus exactly one of the two, so a distributor
working on SKU Mapping never reads NOO rules and vice versa.

The on-screen expander and the plain-text/markdown export both render from
this same structure, so they cannot drift apart. The downloadable file is
the PPTX itself (see load_guide_pptx) — never a separately regenerated
document — so what a user downloads is always byte-identical to the source
of truth, and there is exactly one place this guide's content is authored.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

TITLE_NOO = "Panduan Penginputan — NOO / Store Mapping"
TITLE_SKU = "Panduan Penginputan — SKU Mapping"

#: The single source-of-truth file, served for download as-is.
GUIDE_PPTX_FILENAME = "Panduan Penggunaan Streamlit untuk NOO & SKU MAPPING.pptx"
GUIDE_PPTX_PATH = Path(__file__).parent / GUIDE_PPTX_FILENAME

#: Screenshot assets extracted from the PPTX, repository-relative (never an
#: absolute developer-machine path) so they work in any deployment.
_ASSET_DIR = "assets/guide"


@dataclass(frozen=True)
class GuideItem:
    """One reference bullet, or one numbered walkthrough step.

    `is_step` decides how noo_sku_mapping.py renders it — a step is its own
    plain paragraph (matching the PPTX's "(1) ... (2) ..." numbering), never
    a bulleted list entry, EVEN when it has no screenshot: a step without an
    image (see the NOO step 6 comment below) must still look like the rest
    of its own walkthrough, not fall back to bullet styling just because
    there's no image to anchor it to.

    `image`, when set, is a path relative to the repository root — the
    Streamlit app resolves it against its own file, never an absolute path.
    """
    text: str
    image: str = ""
    is_step: bool = False


def _step(text: str, image_filename: str = "") -> GuideItem:
    image = f"{_ASSET_DIR}/{image_filename}" if image_filename else ""
    return GuideItem(text=text, image=image, is_step=True)


def load_guide_pptx() -> bytes:
    """The bundled source-of-truth PPTX, served unchanged for download.

    Raises FileNotFoundError with a clear message if the asset was not
    shipped with this deployment, rather than silently omitting the download
    button — a missing guide should fail loudly, the same convention used
    for the NOO/SKU upload templates (see sources.load_local_*_template).
    """
    if not GUIDE_PPTX_PATH.is_file():
        raise FileNotFoundError(
            f"Panduan PPTX tidak ditemukan di {GUIDE_PPTX_PATH}. Pastikan "
            f"file '{GUIDE_PPTX_FILENAME}' ikut ter-deploy bersama aplikasi."
        )
    return GUIDE_PPTX_PATH.read_bytes()


#: Applies to both functions. Kept short on purpose.
GENERAL = [
    ("Cara Login", [
        _step(
            "(1) Akses Streamlit dengan menggunakan link berikut: "
            "https://noo-sku-mapping.streamlit.app/.\n\n"
            "(2) Tunggu beberapa saat sampai Streamlit menampilkan halaman "
            "login seperti gambar di samping.\n\n"
            "(3) Login.\n\n"
            "Username : Gunakan kode distributor (DSTxxx) masing-masing.\n\n"
            "Password : 12345678 (password default sementara).",
            "login_1_form.png"),
        _step(
            "(4) Setelah login, pastikan Anda berada pada akun distributor "
            "yang sesuai. Cek pada sebelah kiri atas.\n\n"
            "(5) Demi keamanan akun, silakan mengganti password pada menu "
            "Akun & Password.\n\n"
            "(6) Pastikan Anda menyimpan dan mengingat password baru "
            "tersebut dengan baik, karena password tersebut akan digunakan "
            "saat login.",
            "login_2_ganti_password.png"),
    ]),
    ("Umum", [
        GuideItem("Akun Anda menentukan **perusahaan** yang boleh Anda "
                 "input. Anda tidak dapat mengirim data milik perusahaan "
                 "lain."),
        GuideItem("Jangan mengubah nama kolom, urutan kolom, atau menghapus "
                 "sheet di dalam file template."),
        GuideItem("Sistem menampilkan ringkasan sebelum data masuk. Data "
                 "baru ditulis setelah Anda menekan tombol konfirmasi."),
    ]),
]

NOO = [
    ("Tujuan", [
        GuideItem("NOO / Store Mapping dipakai untuk mendaftarkan toko baru "
                 "(new outlet opening) milik distributor Anda ke tracker."),
    ]),
    ("Langkah-Langkah Upload NOO", [
        _step("(1) Klik Menu NOO/Store Mapping.\n\n(2) Download Template NOO.",
             "noo_1_menu_download.png"),
        _step("(3) Sebelum mengisi template, perhatikan contoh & panduan "
             "pengisian yang tertera di dalam file.",
             "noo_2_template_panduan.png"),
        _step("(4) Setelah template diisi dengan benar, kembali pada "
             "Streamlit dan unggah file pada menu “upload”.",
             "noo_3_upload.png"),
        _step("(5) Setelah file diunggah, klik tombol “Validasi & "
             "Pratinjau”.",
             "noo_4_validasi.png"),
        # (6) The PPT's own screenshot for this step still shows the OLD
        # template's now-removed Store ID validation error — reproducing it
        # here would directly contradict "must not instruct Store ID
        # input", so only the instruction text is kept, verbatim, with no
        # image.
        _step("(6) Setelah menekan tombol “Validasi & Pratinjau,” "
             "Streamlit akan mendeteksi kesalahan jika template yang "
             "diunggah tidak sesuai ketentuan. Harap revisi template sesuai "
             "dengan panduan yang diberikan."),
        _step("(7) Unggah ulang template yang sudah direvisi. Harap menekan "
             "tombol “Validasi & Pratinjau” lagi. Pastikan sudah "
             "0 Error dan 0 Fallback.\n\n"
             "(8) Kemudian, pastikan sudah ada keterangan 1 baris siap "
             "diupload. Lalu klik kotak verifikasi “Saya sudah "
             "memeriksa ringkasan di atas dan setuju melanjutkan "
             "upload.” Terakhir, klik “Konfirmasi & Upload”.",
             "noo_5_ringkasan_konfirmasi.png"),
        _step("(9) DONE, Template NOO berhasil diunggah. Kemudian, harap "
             "konfirmasi kepada BD Support (Intan/Surti).",
             "noo_6_berhasil.png"),
    ]),
    ("Kolom yang Anda isi", [
        GuideItem("**Nama Toko** — wajib, nama toko lengkap dan benar."),
        GuideItem("**Channel (GT / MTI)** — wajib, GT atau MTI."),
        GuideItem("**Nama Cabang** — wajib, nama perusahaan distributor "
                 "Anda."),
        GuideItem("**Kode Brand** — wajib, kode brand (11/13/1A) + "
                 "singkatan distributor Anda."),
        GuideItem("**Kode Cabang** — wajib, kode distributor Anda (contoh: "
                 "DST123)."),
        GuideItem("**Kode Toko Pelanggan** — wajib, gabungan kode cabang + "
                 "customer ID toko (contoh: DST12300010). Jangan mengisi "
                 "customer ID saja."),
        GuideItem("**Kota** — wajib. Ikuti ejaan pada sheet *Kota & Tipe "
                 "Toko*."),
        GuideItem("**Alamat Toko** — wajib, selengkap mungkin: jalan, "
                 "nomor, kelurahan, kecamatan, kota, kode pos."),
        GuideItem("**Tipe Toko** — wajib, harus sesuai channel yang "
                 "dipilih."),
        GuideItem("Template ini **tidak lagi memiliki kolom Store ID** — "
                 "Store ID dilengkapi otomatis oleh sistem berdasarkan "
                 "hasil pengecekan Kode Toko Pelanggan, Anda tidak perlu "
                 "mengisinya."),
        GuideItem("Baris contoh isian pada baris pertama setelah header "
                 "tidak perlu dihapus — sistem otomatis mengabaikannya."),
    ]),
    ("Aturan mapping", [
        GuideItem("Jika satu toko dengan Kode Toko Pelanggan yang sama "
                 "belum terdaftar di lebih dari satu brand, cukup "
                 "**diinput satu kali**."),
        GuideItem("**Satu file boleh berisi beberapa cabang.** Isi **Kode "
                 "Cabang** pada setiap baris dengan kode cabang (DSTxxx) "
                 "yang sesuai untuk toko tersebut."),
        GuideItem("Seluruh cabang yang Anda input harus berada di bawah "
                 "**perusahaan yang sama** dengan akun Anda. Baris dengan "
                 "kode cabang di luar perusahaan Anda akan ditolak dan "
                 "upload dibatalkan."),
        GuideItem("**Kode Toko Pelanggan** harus diawali kode cabang pada "
                 "baris yang sama, bukan kode cabang lain."),
    ]),
    ("Yang diproses sistem setelah Anda upload", [
        GuideItem("Nama Perusahaan dan Kode Cabang pada tracker mengikuti "
                 "data akun Anda untuk cabang yang bersangkutan — bukan "
                 "sekadar apa yang Anda ketik."),
        GuideItem("Sistem memeriksa apakah toko yang Anda input sudah "
                 "pernah terdaftar sebelumnya. Anda tidak perlu melakukan "
                 "pengecekan ini secara manual."),
    ]),
    ("Validasi & hasil", [
        GuideItem("Error ditampilkan per baris: nomor baris, kolom, "
                 "masalah, dan saran perbaikan. Laporan error bisa diunduh "
                 "dalam format Excel."),
        GuideItem("Baris yang isinya persis sama dengan data yang sudah "
                 "pernah diupload akan dilewati; baris lain yang valid "
                 "tetap diproses."),
        GuideItem("Jika toko sudah pernah diupload tetapi ada isi yang "
                 "berubah, baris tersebut dianggap KOREKSI dan tetap "
                 "dimasukkan sebagai baris baru. Data lama tidak diubah."),
        GuideItem("Setelah upload, mohon konfirmasi ke BD Support "
                 "masing-masing distributor (Intan / Surti)."),
    ]),
]

SKU = [
    ("Tujuan", [
        GuideItem("SKU Mapping dipakai untuk memetakan kode produk "
                 "prinsipal ke kode produk milik distributor Anda."),
    ]),
    ("Langkah-Langkah Upload SKU", [
        _step("(1) Klik menu SKU Mapping.\n\n(2) Download Template SKU.",
             "sku_1_menu_download.png"),
        _step("(3) Sebelum mengisi template, perhatikan contoh & panduan "
             "pengisian yang tertera di dalam file.",
             "sku_2_template_panduan.png"),
        _step("(4) Setelah template diisi dengan benar, kembali pada "
             "Streamlit dan unggah file pada menu “upload”.",
             "sku_3_upload.png"),
        _step("(5) Setelah file diunggah, klik tombol “Validasi & "
             "Pratinjau”.",
             "sku_4_validasi.png"),
        _step("(6) Setelah menekan tombol “Validasi & Pratinjau,” "
             "Streamlit akan mendeteksi kesalahan jika template yang "
             "diunggah tidak sesuai ketentuan. Harap revisi template sesuai "
             "dengan panduan yang diberikan.",
             "sku_5_hasil_validasi.png"),
        _step("(7) Unggah ulang template yang sudah direvisi. Harap menekan "
             "tombol “Validasi & Pratinjau” lagi. Pastikan sudah "
             "0 Error dan 0 Fallback.\n\n"
             "(8) Kemudian, pastikan sudah ada keterangan 1 baris siap "
             "diupload. Lalu klik kotak verifikasi “Saya sudah "
             "memeriksa ringkasan di atas dan setuju melanjutkan "
             "upload.” Terakhir, klik “Konfirmasi & Upload”.",
             "sku_6_ringkasan_konfirmasi.png"),
        _step("(9) DONE, Template SKU berhasil diunggah. Kemudian, harap "
             "konfirmasi kepada BD Support (Intan/Surti).",
             "sku_7_berhasil.png"),
    ]),
    ("Kolom yang Anda isi", [
        GuideItem("Template ini hanya berisi **3 kolom**."),
        GuideItem("**Kode SKU Prinsipal** — wajib, **harus terdaftar di "
                 "master produk prinsipal**. Kode yang tidak ditemukan akan "
                 "menggagalkan upload."),
        GuideItem("**Kode SKU Distributor** — wajib, kode produk milik "
                 "distributor."),
        GuideItem("**Nama SKU Distributor** — wajib, nama produk milik "
                 "distributor."),
        GuideItem("**Nama Produk Prinsipal tidak perlu diisi** — kolom ini "
                 "sudah dihapus dari template. Sistem melengkapinya secara "
                 "otomatis dari master produk prinsipal berdasarkan Kode "
                 "SKU Prinsipal yang Anda input."),
    ]),
    ("Aturan mapping", [
        GuideItem("Mapping ini hanya untuk brand **SKINTIFIC, TIMEPHORIA, "
                 "dan FACERINNA**. Produk brand lain akan ditolak."),
        GuideItem("Nama produk prinsipal selalu diambil dari master, bukan "
                 "dari isian Anda."),
        GuideItem("Ukuran / gramasi produk **tidak perlu diisi** — kolom "
                 "tersebut sudah dihapus dari template."),
    ]),
    ("Validasi & hasil", [
        GuideItem("Kode produk yang tidak ditemukan di master prinsipal "
                 "akan ditolak beserta nomor barisnya."),
        GuideItem("Mapping yang persis sama dengan yang sudah pernah "
                 "diupload akan dilewati; baris lain yang valid tetap "
                 "diproses."),
        GuideItem("Jika mapping sudah ada tetapi isinya berubah, baris "
                 "tersebut dianggap KOREKSI dan dimasukkan sebagai baris "
                 "baru."),
        GuideItem("Setelah upload, mohon konfirmasi ke BD Support "
                 "masing-masing distributor (Intan / Surti)."),
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
    """Plain-text rendering — screenshots are omitted (markdown image syntax
    with a local file path does not render in Streamlit); use `sections_for`
    directly to render steps with their images in the app."""
    parts = []
    for heading, items in sections_for(kind):
        parts.append(f"**{heading}**")
        parts.extend(f"- {item.text}" for item in items)
        parts.append("")
    return "\n".join(parts)
