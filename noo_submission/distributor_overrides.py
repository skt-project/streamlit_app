"""Override SEMENTARA untuk data distributor yang belum lengkap di sheet "Distributor Mapping".

ATURAN: sheet SELALU menang. Override hanya mengisi nilai yang KOSONG di sheet, jadi begitu sheet
sudah diisi, override ini otomatis tidak berefek dan boleh dihapus
(hapus file ini + 3 pemanggil di shared.py: muat_distri_code_map, muat_distri_customer_code_raw,
muat_prefix_per_distributor).

Modul ini sengaja tidak memakai Streamlit/network supaya bisa diuji sendiri.

DST366 - PT MENSA BINASUKSES - JAKARTA (join 2026-11-13), dua celah di sheet:
  * "Area Coverage for NOO" = "Jakarta Timur" tidak ada di tab Coverage Mapping / Prefix id.xlsx
    -> distributor tanpa prefix Store ID -> Generate ID gagal "Prefix not found".
  * Company Code kosong -> Customer Code kosong di form NOO bulk dan daftar SKU mapping.
  Nilai: prefix ICWJ = coverage "Jabodetabek" (West Java (SD)), sama dengan delapan distributor Active
  lain di region itu. Company code MBS = kode keempat cabang PT MENSA BINASUKSES lain (DST320-323).
  Keduanya turunan dari pola data, BELUM dikonfirmasi BD Support.
"""

OVERRIDE_DISTRIBUTOR = {
    "DST366": {"company_code": "MBS", "prefix": "ICWJ"},
}


def _kunci(kode) -> str:
    return str(kode or "").strip().upper()


def company_code_efektif(kode_distributor, nilai_sheet) -> str:
    """Company code dari sheet; kalau kosong dan distributornya punya override, pakai override."""
    nilai = str(nilai_sheet or "").strip()
    if nilai:
        return nilai
    return OVERRIDE_DISTRIBUTOR.get(_kunci(kode_distributor), {}).get("company_code", "")


def lengkapi_prefix(prefix_per_nama: dict, nama_per_kode: dict) -> dict:
    """Salinan `prefix_per_nama` (nama distributor -> prefix Store ID) dengan prefix override untuk
    distributor yang BELUM punya prefix. `nama_per_kode` = kode distributor -> nama distributor."""
    hasil = dict(prefix_per_nama)
    for kode, ovr in OVERRIDE_DISTRIBUTOR.items():
        prefix = ovr.get("prefix", "")
        nama = str(nama_per_kode.get(kode, "")).strip()
        if prefix and nama and not str(hasil.get(nama, "")).strip():
            hasil[nama] = prefix
    return hasil
