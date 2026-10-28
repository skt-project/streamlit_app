"""Validasi + kompresi foto evidence: hanya JPG/JPEG, hasil <= 2 MB.

Orientasi tidak diubah: piksel TIDAK diputar berdasarkan EXIF (tanpa `exif_transpose`),
dan tag Orientation dipertahankan apa adanya sehingga tampilan foto sama dengan aslinya.
Metadata EXIF lain (GPS, perangkat, dsb.) sengaja dibuang.
"""
from __future__ import annotations

import io
from dataclasses import dataclass

from PIL import Image, UnidentifiedImageError

MAX_BYTES = 2_000_000
MAX_INPUT_BYTES = 25 * 1024 * 1024
MAX_LONG_SIDE = 2560
_ALLOWED_EXT = (".jpg", ".jpeg")
_ORIENTATION_TAG = 0x0112
_SCALES = (1.0, 0.85, 0.7, 0.55, 0.4, 0.3, 0.2, 0.12)
_Q_MAX, _Q_MIN = 85, 30


class ImageError(ValueError):
    """Foto ditolak; pesan aman ditampilkan ke user."""


@dataclass(frozen=True)
class ProcessedPhoto:
    data: bytes
    width: int
    height: int
    original_size: int
    quality: int
    scale: float

    @property
    def size(self) -> int:
        return len(self.data)


def _encode(img: Image.Image, quality: int, exif: bytes | None) -> bytes:
    buf = io.BytesIO()
    kwargs = {"format": "JPEG", "quality": quality, "optimize": True}
    if exif:
        kwargs["exif"] = exif
    img.save(buf, **kwargs)
    return buf.getvalue()


def process_photo(data: bytes, filename: str | None = None, max_bytes: int = MAX_BYTES) -> ProcessedPhoto:
    """Validasi lalu kompres ke JPEG <= `max_bytes`. Raise `ImageError` bila ditolak."""
    if filename is not None and not filename.lower().endswith(_ALLOWED_EXT):
        raise ImageError("Format foto tidak didukung: hanya JPG/JPEG yang diterima (PNG atau format lain ditolak).")
    if not data:
        raise ImageError("File foto kosong.")
    if len(data) > MAX_INPUT_BYTES:
        raise ImageError(f"File foto terlalu besar (maksimal {MAX_INPUT_BYTES // (1024 * 1024)} MB sebelum kompresi).")

    try:
        img = Image.open(io.BytesIO(data))
        fmt = img.format
        if fmt != "JPEG":
            raise ImageError(
                f"Format foto tidak didukung ({fmt or 'tidak dikenal'}): hanya JPG/JPEG yang diterima."
            )
        orientation = img.getexif().get(_ORIENTATION_TAG)
        img.load()
    except ImageError:
        raise
    except (UnidentifiedImageError, Image.DecompressionBombError, OSError, SyntaxError, ValueError):
        raise ImageError("File bukan foto JPEG yang valid atau rusak.") from None

    exif_bytes = None
    if isinstance(orientation, int) and 1 <= orientation <= 8:
        exif = Image.Exif()
        exif[_ORIENTATION_TAG] = orientation
        exif_bytes = exif.tobytes()

    if img.mode != "RGB" and img.mode != "L":
        img = img.convert("RGB")

    base = img
    long_side = max(img.size)
    if long_side > MAX_LONG_SIDE:
        ratio = MAX_LONG_SIDE / long_side
        base = img.resize((max(1, round(img.width * ratio)), max(1, round(img.height * ratio))), Image.LANCZOS)

    for scale in _SCALES:
        cand = base if scale == 1.0 else base.resize(
            (max(1, round(base.width * scale)), max(1, round(base.height * scale))), Image.LANCZOS)
        out = _encode(cand, _Q_MAX, exif_bytes)
        if len(out) <= max_bytes:
            return ProcessedPhoto(out, cand.width, cand.height, len(data), _Q_MAX, scale)
        if len(_encode(cand, _Q_MIN, exif_bytes)) > max_bytes:
            continue
        lo, hi = _Q_MIN, _Q_MAX
        best = _encode(cand, lo, exif_bytes)
        while hi - lo > 1:
            mid = (lo + hi) // 2
            trial = _encode(cand, mid, exif_bytes)
            if len(trial) <= max_bytes:
                lo, best = mid, trial
            else:
                hi = mid
        return ProcessedPhoto(best, cand.width, cand.height, len(data), lo, scale)

    raise ImageError("Foto tidak dapat dikompres hingga <= 2 MB. Gunakan foto lain.")
