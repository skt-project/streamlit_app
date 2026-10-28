import io
import os
from datetime import date

import pytest
from PIL import Image

from validation import Distributor, Sku

D = date(2026, 10, 23)


@pytest.fixture
def distributors():
    return {
        "D1": Distributor("D1", "Gudang Satu", "R1", "Ani"),
        "D2": Distributor("D2", "Gudang Dua", "R1", "Budi"),
        "D3": Distributor("D3", "Gudang Tiga", "R2", "Ani"),
    }


@pytest.fixture
def skus():
    return {
        "TST000001": Sku("TST000001", "Produk Contoh Satu"),
        "TST000002": Sku("TST000002", "Produk Contoh Dua"),
        "TST000003": Sku("TST000003", "Produk Contoh Tiga"),
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
