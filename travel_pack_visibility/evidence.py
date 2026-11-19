"""Simpan foto display ke Google Cloud Storage (bucket privat).

Objek: `<prefix>/<Nama ASM>/<KODE> - <NAMA TOKO> - <YYYYmmdd_HHMMSS>_<id>.<jpg|png>`

Setiap submit menjadi objek BARU (waktu + id submit ada di nama, dan upload memakai
if_generation_match=0). Aplikasi ini tidak punya fungsi hapus atau timpa; cukup role
Storage Object Creator untuk service account.

Link `https://storage.cloud.google.com/...` hanya bisa dibuka akun Google yang punya role
Storage Object Viewer pada bucket (atau di atasnya).
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import quote

from google.api_core.exceptions import Forbidden, GoogleAPICallError, NotFound, PreconditionFailed

from timeutil import to_wib

log = logging.getLogger("travel_pack.evidence")

_UNSAFE = re.compile(r'[\\/\x00-\x1f#?\[\]*]')


class EvidenceError(Exception):
    """Upload foto gagal. Pesan aman ditampilkan ke user."""


@dataclass(frozen=True)
class StoredPhoto:
    object_name: str
    url: str
    file_name: str
    gs_uri: str


def safe_name(value: str, limit: int = 80) -> str:
    return _UNSAFE.sub("_", " ".join(str(value).split()))[:limit].strip() or "_"


def object_url(bucket: str, object_name: str) -> str:
    return f"https://storage.cloud.google.com/{bucket}/{quote(object_name, safe='/')}"


class GcsEvidence:
    def __init__(self, client, bucket_name: str, prefix: str):
        self.bucket_name = bucket_name
        self.prefix = safe_name(prefix, 120)
        self.bucket = client.bucket(bucket_name)        # tidak memanggil API

    def object_name_for(self, *, asm: str, store_code: str, store_name: str, submitted_at: datetime,
                        submission_id: str, extension: str) -> tuple[str, str]:
        stamp = to_wib(submitted_at).strftime("%Y%m%d_%H%M%S")
        base = f"{store_code} - {store_name}" if store_name else store_code
        file_name = f"{safe_name(base)} - {stamp}_{submission_id[:8]}{extension}"
        return f"{self.prefix}/{safe_name(asm)}/{file_name}", file_name

    def upload(self, data: bytes, *, content_type: str, extension: str, asm: str, store_code: str,
               store_name: str, spv: str, submitted_at: datetime, submission_id: str) -> StoredPhoto:
        object_name, file_name = self.object_name_for(
            asm=asm, store_code=store_code, store_name=store_name, submitted_at=submitted_at,
            submission_id=submission_id, extension=extension)
        blob = self.bucket.blob(object_name)
        blob.metadata = {
            "asm": asm, "store_code": store_code, "store_name": store_name, "spv": spv,
            "submission_id": submission_id,
            "submitted_at": to_wib(submitted_at).strftime("%Y-%m-%d %H:%M:%S") + " WIB",
        }
        try:
            blob.upload_from_string(data, content_type=content_type, if_generation_match=0)
        except PreconditionFailed as exc:
            raise EvidenceError("Nama file foto bentrok dengan file lama; upload dibatalkan. Coba submit lagi.") from exc
        except Forbidden as exc:
            log.exception("GCS: akses ditolak (%s)", object_name)
            raise EvidenceError("Upload foto ditolak: service account belum punya akses tulis ke bucket.") from exc
        except NotFound as exc:
            raise EvidenceError("Upload foto gagal: bucket penyimpanan tidak ditemukan.") from exc
        except (GoogleAPICallError, OSError) as exc:
            log.exception("GCS: upload gagal (%s)", object_name)
            raise EvidenceError(f"Upload foto gagal ({exc.__class__.__name__}). Cek sinyal lalu coba lagi.") from exc
        return StoredPhoto(object_name=object_name, url=object_url(self.bucket_name, object_name),
                           file_name=file_name, gs_uri=f"gs://{self.bucket_name}/{object_name}")
