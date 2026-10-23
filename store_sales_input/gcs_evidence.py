"""Simpan foto evidence ke Google Cloud Storage.

Objek: `<root>/<YYYY-MM>/<store_id> - <store_name>/<tanggal>_<store_id>_<sku>_<transaction_id>_<waktu upload>.jpg`

Nama objek memuat waktu upload (WIB) sehingga setiap upload menjadi objek BARU. Dengan begitu
mengganti foto tidak menimpa objek lama. Aplikasi ini TIDAK PERNAH menghapus atau menimpa objek
(tidak ada fungsi hapus; upload memakai if_generation_match=0). Foto lama dibiarkan di bucket
sampai ada yang memerintahkan penghapusan. Service account hanya diberi role Storage Object Creator.

Bucket bersifat privat. Link `https://storage.cloud.google.com/...` hanya bisa dibuka akun Google
yang diberi role Storage Object Viewer pada bucket.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import timedelta
from datetime import date, datetime, timezone
from urllib.parse import quote

from google.api_core.exceptions import Forbidden, GoogleAPICallError, NotFound, PreconditionFailed

from timeutil import to_wib

log = logging.getLogger("store_sales.gcs")

_UNSAFE_NAME = re.compile(r'[\\/\x00-\x1f]')


class EvidenceStorageError(Exception):
    """Operasi penyimpanan evidence gagal. Pesan aman ditampilkan ke user."""


@dataclass(frozen=True)
class EvidenceFile:
    file_id: str            # nama objek lengkap di bucket
    url: str
    name: str               # nama file (tanpa folder)
    uploaded_at: datetime


def safe_name(value: str, limit: int = 150) -> str:
    return _UNSAFE_NAME.sub("_", " ".join(str(value).split()))[:limit].strip() or "_"


def object_url(bucket: str, object_name: str) -> str:
    return f"https://storage.cloud.google.com/{bucket}/{quote(object_name, safe='/')}"


class GcsEvidence:
    def __init__(self, client, bucket_name: str, root_prefix: str = "Store Sales Evidence"):
        self.bucket_name = bucket_name
        self.root = safe_name(root_prefix)
        # `bucket()` tidak memanggil API, jadi service account cukup dengan izin objek.
        self.bucket = client.bucket(bucket_name)

    def _explain(self, exc: Exception, what: str) -> EvidenceStorageError:
        if isinstance(exc, Forbidden):
            return EvidenceStorageError(
                f"Gagal {what}: akses ditolak. Pastikan service account punya role Storage Object Creator "
                f"pada bucket {self.bucket_name}.")
        if isinstance(exc, NotFound):
            return EvidenceStorageError(f"Gagal {what}: bucket {self.bucket_name} tidak ditemukan.")
        return EvidenceStorageError(f"Gagal {what}: {exc.__class__.__name__}.")

    def upload(self, *, data: bytes, sales_date: date, store_id: str, store_name: str,
               sku_code: str, sku_name: str, supervisor_name: str, transaction_id: str) -> EvidenceFile:
        uploaded_at = datetime.now(timezone.utc)
        stamp = to_wib(uploaded_at).strftime("%Y%m%d%H%M%S")
        name = safe_name(f"{sales_date.isoformat()}_{store_id}_{sku_code}_{transaction_id}_{stamp}") + ".jpg"
        folder = f"{self.root}/{sales_date.strftime('%Y-%m')}/{safe_name(f'{store_id} - {store_name}')}"
        object_name = f"{folder}/{name}"
        blob = self.bucket.blob(object_name)
        blob.metadata = {
            "store_id": store_id,
            "store_name": store_name,
            "sku_code": sku_code,
            "sku_name": sku_name,
            "tanggal_penjualan": sales_date.isoformat(),
            "supervisor_name": supervisor_name,
            "uploaded_at": to_wib(uploaded_at).strftime("%Y-%m-%d %H:%M:%S") + " WIB",
            "transaction_id": transaction_id,
        }
        try:
            blob.upload_from_string(data, content_type="image/jpeg", if_generation_match=0)
        except PreconditionFailed as exc:
            raise EvidenceStorageError(f"Objek {name} sudah ada; upload dibatalkan agar tidak menimpa. Coba submit ulang.") from exc
        except (GoogleAPICallError, OSError) as exc:
            log.exception("GCS: upload gagal (%s)", object_name)
            raise self._explain(exc, f"upload foto {name}") from exc
        return EvidenceFile(file_id=object_name, url=object_url(self.bucket_name, object_name),
                            name=name, uploaded_at=uploaded_at)

    # ── Baca (hanya untuk tab Admin; tetap tidak ada fungsi hapus) ──────────

    def view_url(self, file_id: str, minutes: int = 60) -> str | None:
        """URL bertanda tangan (V4, hanya-baca) yang kedaluwarsa dalam `minutes` menit, agar foto di bucket
        privat bisa ditampilkan di galeri tanpa membuka bucket. None bila kredensial tidak bisa menandatangani
        (mis. Application Default Credentials tanpa private key) → pakai `read_bytes`."""
        try:
            return self.bucket.blob(file_id).generate_signed_url(
                version="v4", expiration=timedelta(minutes=minutes), method="GET")
        except Exception:
            log.warning("GCS: gagal membuat signed URL untuk %s", file_id, exc_info=True)
            return None

    def read_bytes(self, file_id: str) -> bytes:
        try:
            return self.bucket.blob(file_id).download_as_bytes()
        except (GoogleAPICallError, OSError) as exc:
            log.exception("GCS: gagal membaca %s", file_id)
            raise self._explain(exc, f"membaca foto {file_id}") from exc
