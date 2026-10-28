import io

import pytest
from PIL import Image

from conftest import make_jpeg
from image_processor import MAX_BYTES, ImageError, process_photo


def test_large_noisy_jpeg_compressed_under_2mb():
    big = make_jpeg((3000, 2000), quality=95, noise=True)
    assert len(big) > 3_000_000
    out = process_photo(big, "foto.jpg")
    assert out.size <= MAX_BYTES == 2_000_000
    assert Image.open(io.BytesIO(out.data)).format == "JPEG"
    assert out.original_size == len(big)


def test_png_named_jpg_and_corrupt_and_empty_are_rejected():
    png = make_jpeg(fmt="PNG")
    with pytest.raises(ImageError, match="JPG/JPEG"):
        process_photo(png, "trick.jpg")
    with pytest.raises(ImageError, match="JPG/JPEG"):
        process_photo(make_jpeg(), "x.png")
    with pytest.raises(ImageError):
        process_photo(b"not an image", "a.jpg")
    with pytest.raises(ImageError, match="kosong"):
        process_photo(b"", "a.jpg")


def test_orientation_tag_kept_pixels_not_rotated_and_other_exif_dropped():
    src = make_jpeg((200, 100), exif_orientation=6)
    out = process_photo(src, "a.jpg")
    img = Image.open(io.BytesIO(out.data))
    assert img.size == (200, 100)
    assert img.getexif().get(0x0112) == 6
    original = Image.new("RGB", (50, 50))
    exif = Image.Exif()
    exif[0x0112] = 1
    exif[0x010F] = "SecretMaker"
    buf = io.BytesIO()
    original.save(buf, "JPEG", exif=exif.tobytes())
    cleaned = Image.open(io.BytesIO(process_photo(buf.getvalue(), "a.jpg").data))
    assert cleaned.getexif().get(0x0112) == 1
    assert 0x010F not in cleaned.getexif()
