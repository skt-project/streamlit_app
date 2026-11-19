"""Validasi foto display + baca metadata EXIF (waktu & lokasi) bila ada.

Foto disimpan APA ADANYA (tidak dikompres) supaya cap lokasi dari aplikasi GPS camera tetap terbaca.
Batas 10 MB per foto, sama dengan Google Form. Metadata EXIF hanya dicatat, tidak dipakai menolak foto:
banyak HP / browser (terutama iPhone) membuang lokasi EXIF saat foto dipilih dari galeri.
"""
from __future__ import annotations

import io
from dataclasses import dataclass

from PIL import Image, ImageOps, UnidentifiedImageError

MAX_BYTES = 10 * 1024 * 1024
ACCEPTED_TYPES = ["jpg", "jpeg", "png"]
# Format PIL -> (content type, ekstensi). MPO = JPEG multi-gambar dari sebagian HP (tetap JPEG biasa).
_FORMATS = {"JPEG": ("image/jpeg", ".jpg"), "MPO": ("image/jpeg", ".jpg"), "PNG": ("image/png", ".png")}

_EXIF_IFD = 0x8769
_GPS_IFD = 0x8825
_DATETIME_ORIGINAL = 36867
_DATETIME = 306


class PhotoError(Exception):
    """Foto tidak valid. Pesan aman ditampilkan ke user."""


@dataclass(frozen=True)
class PhotoInfo:
    content_type: str
    extension: str
    width: int
    height: int
    size: int
    taken_at: str = ""       # "2026-11-19 10:15:00" dari EXIF, kosong bila tidak ada
    gps: str = ""            # "-6.200000, 106.816666" dari EXIF, kosong bila tidak ada


def human_size(n: int) -> str:
    return f"{n / 1024:.0f} KB" if n < 1024 * 1024 else f"{n / 1024 / 1024:.1f} MB"


def _ratio(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError, ZeroDivisionError):
        num, den = v
        return float(num) / float(den)


def _dms(values, ref: str) -> float:
    d, m, s = (_ratio(x) for x in values)
    deg = d + m / 60 + s / 3600
    return -deg if str(ref).strip().upper() in ("S", "W") else deg


def _read_exif(img: Image.Image) -> tuple[str, str]:
    try:
        exif = img.getexif()
    except Exception:
        return "", ""
    taken = ""
    try:
        raw = exif.get_ifd(_EXIF_IFD).get(_DATETIME_ORIGINAL) or exif.get(_DATETIME)
        if raw:
            text = str(raw).strip().strip("\x00")
            # format EXIF "YYYY:MM:DD HH:MM:SS"
            taken = text[:10].replace(":", "-") + text[10:] if len(text) >= 19 else text
    except Exception:
        taken = ""
    gps = ""
    try:
        g = exif.get_ifd(_GPS_IFD)
        if g and 2 in g and 4 in g:
            lat = _dms(g[2], g.get(1, "N"))
            lon = _dms(g[4], g.get(3, "E"))
            if -90 <= lat <= 90 and -180 <= lon <= 180 and not (lat == 0 and lon == 0):
                gps = f"{lat:.6f}, {lon:.6f}"
    except Exception:
        gps = ""
    return taken, gps


def thumbnail(data: bytes, max_side: int = 900) -> bytes:
    """Pratinjau kecil (JPEG) supaya foto 10 MB tidak dikirim balik utuh ke HP. Arah foto mengikuti EXIF."""
    with Image.open(io.BytesIO(data)) as img:
        img = ImageOps.exif_transpose(img)
        img.thumbnail((max_side, max_side))
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=80)
        return buf.getvalue()


def inspect_photo(data: bytes) -> PhotoInfo:
    size = len(data or b"")
    if size == 0:
        raise PhotoError("File foto kosong.")
    if size > MAX_BYTES:
        raise PhotoError(f"Ukuran foto {human_size(size)}, maksimal 10 MB.")
    try:
        with Image.open(io.BytesIO(data)) as img:
            fmt = (img.format or "").upper()
            if fmt not in _FORMATS:
                raise PhotoError(f"Format {fmt or 'tidak dikenal'} tidak didukung. Gunakan JPG atau PNG.")
            img.verify()                     # harus dipanggil langsung setelah open
        with Image.open(io.BytesIO(data)) as img:
            width, height = img.size
            taken, gps = _read_exif(img)
            img.load()                       # file terpotong (upload putus) gagal di sini
    except PhotoError:
        raise
    except (UnidentifiedImageError, Image.DecompressionBombError) as exc:
        raise PhotoError("File bukan foto yang valid (harus JPG atau PNG).") from exc
    except Exception as exc:
        raise PhotoError("Foto rusak / tidak bisa dibaca. Coba ambil ulang fotonya.") from exc
    content_type, ext = _FORMATS[fmt]
    return PhotoInfo(content_type=content_type, extension=ext, width=width, height=height, size=size,
                     taken_at=taken, gps=gps)
