import io
import os
from datetime import date

import pytest
from PIL import Image

from validation import Sku, Store

D = date(2026, 10, 23)


@pytest.fixture
def stores():
    return {
        "ST000001": Store("ST000001", "Toko Contoh 3", "JABODEBEK", "GT", "Non BA"),
        "ST000002": Store("ST000002", "Toko Dua", "JATENG", "GT", "Non BA"),
    }


@pytest.fixture
def skus():
    return {
        "SKU000001": Sku("SKU000001", "Skin Barrier Cream 20g"),
        "SKU000002": Sku("SKU000002", "Sunscreen 30g"),
        "SKU000003": Sku("SKU000003", "Serum 15ml"),
    }


def make_jpeg(size=(200, 100), quality=90, noise=False, exif_orientation=None, fmt="JPEG") -> bytes:
    if noise:
        img = Image.frombytes("RGB", size, os.urandom(size[0] * size[1] * 3))
    else:
        img = Image.new("RGB", size, (200, 30, 30))
    buf = io.BytesIO()
    kwargs = {}
    if exif_orientation:
        exif = Image.Exif()
        exif[0x0112] = exif_orientation
        kwargs["exif"] = exif.tobytes()
    if fmt == "JPEG":
        kwargs["quality"] = quality
    img.save(buf, format=fmt, **kwargs)
    return buf.getvalue()
