import io

import pytest
from PIL import Image

from conftest import make_jpeg
from image_processor import MAX_BYTES, ImageError, process_photo


def test_large_noisy_jpeg_compressed_under_2mb():
    big = make_jpeg((3000, 2000), quality=95, noise=True)
    assert len(big) > 3_000_000
    out = process_photo(big, "foto.jpg")
    assert out.size <= MAX_BYTES == 2_000_000 < 2 * 1024 * 1024
    assert Image.open(io.BytesIO(out.data)).format == "JPEG"
    assert out.original_size == len(big)


def test_small_jpeg_kept_valid():
    out = process_photo(make_jpeg((200, 100)), "a.JPEG")
    assert Image.open(io.BytesIO(out.data)).format == "JPEG" and out.size <= MAX_BYTES


def test_camera_bytes_without_filename_accepted():
    assert process_photo(make_jpeg((64, 64))).size > 0


@pytest.mark.parametrize("name", ["x.png", "x.gif", "x.webp", "x.heic", "x"])
def test_non_jpeg_extension_rejected(name):
    with pytest.raises(ImageError, match="JPG/JPEG"):
        process_photo(make_jpeg(), name)


def test_png_content_rejected_even_when_named_jpg():
    png = make_jpeg(fmt="PNG")
    assert Image.open(io.BytesIO(png)).format == "PNG"
    with pytest.raises(ImageError, match="JPG/JPEG"):
        process_photo(png, "trick.jpg")


def test_corrupt_and_empty_rejected():
    with pytest.raises(ImageError):
        process_photo(b"not an image at all", "a.jpg")
    with pytest.raises(ImageError):
        process_photo(b"", "a.jpg")


def test_exif_orientation_not_applied_but_tag_preserved():
    src = make_jpeg((200, 100), exif_orientation=6)             # piksel landscape + tag "putar 90"
    out = process_photo(src, "a.jpg")
    img = Image.open(io.BytesIO(out.data))
    assert img.size == (200, 100)                                # piksel TIDAK diputar
    assert img.getexif().get(0x0112) == 6                        # tag tetap → tampilan sama dengan aslinya


def test_other_exif_stripped():
    img = Image.new("RGB", (50, 50))
    exif = Image.Exif()
    exif[0x0112] = 1
    exif[0x010F] = "SecretMaker"
    buf = io.BytesIO()
    img.save(buf, "JPEG", exif=exif.tobytes())
    out = Image.open(io.BytesIO(process_photo(buf.getvalue(), "a.jpg").data))
    assert 0x010F not in out.getexif()
