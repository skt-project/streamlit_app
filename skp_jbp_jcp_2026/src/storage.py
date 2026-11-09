"""Document storage: Google Cloud Storage (live) or a local folder (mock).

Object names are deterministic ('2026 Q1/jbp/{region}/{file}.pdf'), so an
upload of the same combination overwrites the previous object.
"""
from __future__ import annotations

import io
import logging
import os
import zipfile
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Callable, Iterable, Protocol

log = logging.getLogger("skp.storage")

MAX_ZIP_FILES = 500
MAX_ZIP_BYTES = 300 * 1024 * 1024


class StorageError(RuntimeError):
    """Error with a message that is safe to show to end users."""


def object_name(location: str) -> str:
    """Object name inside the bucket from what the log's 'GCS Path' holds.

    'gs://bucket/2026 Q1/jbp/R/f.pdf' -> '2026 Q1/jbp/R/f.pdf'
    'local://2026 Q1/jbp/R/f.pdf'     -> '2026 Q1/jbp/R/f.pdf'
    '2026 Q1/jbp/R/f.pdf' (older rows) -> unchanged
    """
    value = str(location or "").strip()
    if value.startswith("gs://"):
        rest = value[len("gs://"):]
        return rest.split("/", 1)[1] if "/" in rest else ""
    if value.startswith("local://"):
        return value[len("local://"):]
    return value


class DocumentStorage(Protocol):
    label: str
    supports_links: bool

    def uri(self, path: str) -> str: ...

    def upload_pdf(self, path: str, data: bytes) -> None: ...

    def download(self, path: str) -> bytes | None: ...

    def delete(self, path: str) -> bool: ...

    def signed_url(self, path: str, minutes: int = 15) -> str | None: ...

    def check(self) -> tuple[bool, str]: ...


def _friendly_gcs_error(exc: Exception, bucket: str) -> StorageError:
    from google.api_core import exceptions as gexc

    if isinstance(exc, gexc.NotFound):
        return StorageError(f"Bucket GCS '{bucket}' tidak ditemukan. Periksa secret GCS_BUCKET.")
    if isinstance(exc, gexc.Forbidden):
        return StorageError(
            f"Service account tidak punya izin menulis ke bucket '{bucket}'. "
            "Berikan role Storage Object Admin pada bucket tersebut."
        )
    return StorageError(f"Gagal menghubungi Google Cloud Storage ({type(exc).__name__}). Coba lagi beberapa saat.")


class GCSStorage:
    supports_links = True

    def __init__(self, client, bucket_name: str):
        self._bucket = client.bucket(bucket_name)
        self._bucket_name = bucket_name
        self.label = f"gs://{bucket_name}"

    def uri(self, path: str) -> str:
        """Full location written to the log, e.g. gs://bucket/2026 Q1/jbp/Region/file.pdf."""
        return f"gs://{self._bucket_name}/{path}"

    def upload_pdf(self, path: str, data: bytes) -> None:
        from google.cloud.storage.retry import DEFAULT_RETRY

        blob = self._bucket.blob(path)
        try:
            # Unconditional overwrite is intended (re-upload replaces the file);
            # retrying is safe because the same bytes go to the same name.
            blob.upload_from_string(data, content_type="application/pdf", retry=DEFAULT_RETRY, timeout=120)
        except Exception as exc:  # noqa: BLE001 - translated for the user
            log.warning("GCS upload failed for %s: %s", path, type(exc).__name__)
            raise _friendly_gcs_error(exc, self._bucket_name) from exc

    def download(self, path: str) -> bytes | None:
        from google.api_core import exceptions as gexc

        try:
            return self._bucket.blob(path).download_as_bytes(timeout=120)
        except gexc.NotFound:
            return None
        except Exception as exc:  # noqa: BLE001
            raise _friendly_gcs_error(exc, self._bucket_name) from exc

    def delete(self, path: str) -> bool:
        from google.api_core import exceptions as gexc

        try:
            self._bucket.blob(path).delete(timeout=60)
            return True
        except gexc.NotFound:
            return False

    def signed_url(self, path: str, minutes: int = 15) -> str | None:
        try:
            return self._bucket.blob(path).generate_signed_url(
                version="v4", expiration=timedelta(minutes=minutes), method="GET"
            )
        except Exception as exc:  # noqa: BLE001 - e.g. credentials without a signer
            log.info("Signed URL unavailable: %s", type(exc).__name__)
            return None

    def check(self) -> tuple[bool, str]:
        try:
            next(iter(self._bucket.client.list_blobs(self._bucket_name, max_results=1)), None)
            return True, f"Bucket {self.label} dapat diakses."
        except Exception as exc:  # noqa: BLE001
            return False, str(_friendly_gcs_error(exc, self._bucket_name))


class LocalStorage:
    """Stores files under a local folder with the same object layout as GCS."""

    supports_links = False

    def __init__(self, root: str | os.PathLike):
        self._root = Path(root).resolve()
        self.label = f"folder lokal …/{self._root.parent.name}/{self._root.name}"

    def uri(self, path: str) -> str:
        return f"local://{path}"

    def _resolve(self, path: str) -> Path:
        target = (self._root / path).resolve()
        if self._root not in target.parents:
            raise StorageError("Path dokumen tidak valid.")
        return target

    def upload_pdf(self, path: str, data: bytes) -> None:
        target = self._resolve(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(".part")
        tmp.write_bytes(data)
        os.replace(tmp, target)

    def download(self, path: str) -> bytes | None:
        target = self._resolve(path)
        return target.read_bytes() if target.exists() else None

    def delete(self, path: str) -> bool:
        target = self._resolve(path)
        if target.exists():
            target.unlink()
            return True
        return False

    def signed_url(self, path: str, minutes: int = 15) -> str | None:
        return None

    def check(self) -> tuple[bool, str]:
        return True, f"Mode lokal: file disimpan di {self._root}"


@dataclass
class ZipResult:
    data: bytes
    added: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    total_bytes: int = 0


def build_zip(
    paths: Iterable[str],
    storage: DocumentStorage,
    max_files: int = MAX_ZIP_FILES,
    max_bytes: int = MAX_ZIP_BYTES,
    progress: Callable[[int, int], None] | None = None,
) -> ZipResult:
    """Build a ZIP in memory (io.BytesIO). Entry names are the object paths, so
    the folder layout is '2026 Qx/jbp|jcp/{region}/{file}.pdf'. Nothing is
    written to GCS or to disk."""
    # Log rows hold gs://bucket/...; the ZIP keeps the in-bucket layout '2026 Qx/jbp|jcp/{region}/...'.
    unique = sorted({object_name(p) for p in paths if object_name(p)})
    if len(unique) > max_files:
        raise StorageError(
            f"Terlalu banyak dokumen untuk satu ZIP ({len(unique)}). Maksimal {max_files}; persempit filter."
        )
    buffer = io.BytesIO()
    result = ZipResult(data=b"")
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for i, path in enumerate(unique, start=1):
            data = storage.download(path)
            if data is None:
                result.missing.append(path)
            else:
                result.total_bytes += len(data)
                if result.total_bytes > max_bytes:
                    raise StorageError("Ukuran ZIP melebihi batas 300 MB. Persempit filter lalu coba lagi.")
                zf.writestr(path.lstrip("/"), data)
                result.added.append(path)
            if progress:
                progress(i, len(unique))
    result.data = buffer.getvalue()
    return result
