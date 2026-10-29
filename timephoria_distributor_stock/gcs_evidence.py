"""Simpan foto evidence ke Google Cloud Storage.

Objek: `<root>/<YYYY-MM>/<distributor_code> - <nama>/<tanggal>_<kode>_<sku>_<transaction_id>_<waktu upload WIB>.jpg`

Waktu upload (WIB) ada di nama supaya setiap upload menjadi objek BARU. Aplikasi ini TIDAK PERNAH
menghapus atau menimpa objek: tidak ada fungsi hapus, dan upload memakai if_generation_match=0.
Foto lama tetap di bucket. Service account hanya Object Creator + Object Viewer.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote

from google.api_core.exceptions import Forbidden, GoogleAPICallError, NotFound, PreconditionFailed

from timeutil import to_wib

log = logging.getLogger("timephoria.gcs")

_UNSAFE_NAME = re.compile(r"[\\/\x00-\x1f]")


class EvidenceStorageError(Exception):
    """Operasi penyimpanan evidence gagal. Pesan aman ditampilkan ke user."""


@dataclass(frozen=True)
class EvidenceFile:
    file_id: str
    url: str
    name: str
    uploaded_at: datetime


def safe_name(value: str, limit: int = 150) -> str:
    return _UNSAFE_NAME.sub("", " ".join(str(value).split()))[:limit].strip() or "_"


def object_url(bucket: str, object_name: str) -> str:
    return f"https://storage.cloud.google.com/{bucket}/{quote(object_name, safe='/')}"


class GcsEvidence:
    def __init__(self, client, bucket_name: str, root_prefix: str = "Timephoria Distributor Stock Evidence"):
        self.bucket_name = bucket_name
        self.root = safe_name(root_prefix)
        self.bucket = client.bucket(bucket_name)

    def _explain(self, exc: Exception, what: str) -> EvidenceStorageError:
        if isinstance(exc, Forbidden):
            return EvidenceStorageError(
                f"Gagal {what}: akses ditolak. Pastikan service account punya role Storage Object Creator "
                f"dan Storage Object Viewer pada bucket {self.bucket_name}.")
        if isinstance(exc, NotFound):
            return EvidenceStorageError(f"Gagal {what}: bucket {self.bucket_name} tidak ditemukan.")
        return EvidenceStorageError(f"Gagal {what}: {exc.__class__.__name__}.")

    def upload(
        self, *, data: bytes, tanggal_stock: date, distributor_code: str, distributor_name: str,
        spu_code: str, spu_name: str, supervisor_name: str, transaction_id: str,
    ) -> EvidenceFile:
        uploaded_at = datetime.now(timezone.utc)
        stamp = to_wib(uploaded_at).strftime("%Y%m%d%H%M%S")
        name = safe_name(
            f"{tanggal_stock.isoformat()}_{distributor_code}_{spu_code}_{transaction_id}_{stamp}"
        ) + ".jpg"
        folder = (
            f"{self.root}/{tanggal_stock.strftime('%Y-%m')}/"
            f"{safe_name(f'{distributor_code} - {distributor_name}')}"
        )
        object_name = f"{folder}/{name}"
        blob = self.bucket.blob(object_name)
        blob.metadata = {
            "distributor_code": distributor_code,
            "distributor_name": distributor_name,
            "spu_code": spu_code,
            "spu_name": spu_name,
            "tanggal_stock": tanggal_stock.isoformat(),
            "supervisor_name": supervisor_name,
            "uploaded_at": to_wib(uploaded_at).strftime("%Y-%m-%d %H:%M:%S") + " WIB",
            "transaction_id": transaction_id,
        }
        try:
            blob.upload_from_string(data, content_type="image/jpeg", if_generation_match=0)
        except PreconditionFailed as exc:
            raise EvidenceStorageError(
                f"Objek {name} sudah ada, coba submit ulang. Upload dibatalkan agar tidak menimpa."
            ) from exc
        except (GoogleAPICallError, OSError) as exc:
            log.exception("GCS: upload gagal (%s)", object_name)
            raise self._explain(exc, f"upload foto {name}") from exc
        return EvidenceFile(
            file_id=object_name, url=object_url(self.bucket_name, object_name),
            name=name, uploaded_at=uploaded_at,
        )

    def view_url(self, file_id: str, minutes: int = 60) -> str | None:
        """URL bertanda tangan V4 (hanya baca) yang kedaluwarsa dalam `minutes` menit.

        None bila kredensial tidak bisa menandatangani → pemanggil memakai `read_bytes`.
        """
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
