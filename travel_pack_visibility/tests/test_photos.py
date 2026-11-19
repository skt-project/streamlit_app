import io

import pytest
from PIL import Image

from conftest import make_jpeg
from photos import MAX_BYTES, PhotoError, inspect_photo, thumbnail


def test_jpeg_with_exif_gps_and_time():
    info = inspect_photo(make_jpeg(gps=(-6.2, 106.816666), taken="2026:11:19 10:15:00"))
    assert info.content_type == "image/jpeg" and info.extension == ".jpg"
    assert (info.width, info.height) == (320, 200)
    assert info.taken_at == "2026-11-19 10:15:00"
    assert info.gps == "-6.200000, 106.816666"


def test_jpeg_without_exif():
    info = inspect_photo(make_jpeg())
    assert info.taken_at == "" and info.gps == ""


def test_png_accepted():
    info = inspect_photo(make_jpeg(fmt="PNG"))
    assert info.content_type == "image/png" and info.extension == ".png"


def test_gif_rejected():
    buf = io.BytesIO()
    Image.new("RGB", (10, 10)).save(buf, format="GIF")
    with pytest.raises(PhotoError, match="GIF"):
        inspect_photo(buf.getvalue())


@pytest.mark.parametrize("data,msg", [
    (b"", "kosong"),
    (b"not an image at all", "bukan foto"),
])
def test_invalid_bytes(data, msg):
    with pytest.raises(PhotoError, match=msg):
        inspect_photo(data)


def test_truncated_jpeg_rejected():
    data = make_jpeg(size=(800, 600))
    with pytest.raises(PhotoError):
        inspect_photo(data[: len(data) // 2])


def test_too_big_rejected():
    with pytest.raises(PhotoError, match="maksimal 10 MB"):
        inspect_photo(b"\xff" * (MAX_BYTES + 1))


def test_thumbnail_is_small_jpeg():
    out = thumbnail(make_jpeg(size=(3000, 2000)), max_side=900)
    with Image.open(io.BytesIO(out)) as img:
        assert img.format == "JPEG" and max(img.size) == 900
