"""Alur submit: validasi -> cek tab hasil -> upload foto ke GCS -> tambah 1 baris di tab hasil."""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from evidence import EvidenceError, StoredPhoto
from photos import PhotoError, PhotoInfo, inspect_photo
from responses import STATUS_AGAIN, STATUS_FIRST, ResponseSheetError
from stores import Store, StoreList, clean, fmt_number
from timeutil import fmt_wib, now_utc

log = logging.getLogger("travel_pack.service")

MAX_SPV_LEN = 80


class SubmitRejected(Exception):
    """Input tidak valid; tidak ada yang disimpan."""


class SubmitFailed(Exception):
    """Gagal di tengah jalan. `photo_saved` = foto sudah masuk bucket tapi baris belum tertulis."""

    def __init__(self, message: str, photo_saved: bool = False):
        super().__init__(message)
        self.photo_saved = photo_saved


@dataclass(frozen=True)
class SubmitResult:
    submission_id: str
    submitted_at: str
    asm: str
    store: Store
    spv: str
    photo: StoredPhoto
    info: PhotoInfo


def clean_spv(value: str) -> str:
    return clean(value)[:MAX_SPV_LEN]


class Submitter:
    def __init__(self, evidence, responses, clock: Callable[[], datetime] = now_utc):
        self.evidence = evidence
        self.responses = responses
        self.clock = clock

    def submit(self, *, store_list: StoreList, asm: str, store_code: str, spv: str, photo: bytes | None
               ) -> SubmitResult:
        asm = clean(asm)
        if not asm:
            raise SubmitRejected("Pilih Nama ASM.")
        store = store_list.get(store_code or "")
        if store is None:
            raise SubmitRejected("Pilih toko dari daftar.")
        if store.asm.casefold() != asm.casefold():
            raise SubmitRejected(f"Toko {store.code} bukan toko ASM {asm}.")
        if not store_list.is_eligible(store):
            raise SubmitRejected(f"Toko {store.code} belum memenuhi syarat Ach SKU (MOQ) >= "
                                 f"{fmt_number(store_list.min_ach)}.")
        if not photo:
            raise SubmitRejected("Foto display toko wajib diupload.")
        try:
            info = inspect_photo(photo)
        except PhotoError as exc:
            raise SubmitRejected(str(exc)) from exc
        spv = clean_spv(spv)

        # Tab hasil dibaca dulu (status upload ulang + cek header) supaya foto tidak terkirim ke bucket
        # bila sheet sedang tidak bisa ditulis.
        try:
            prior = self.responses.upload_index().get(store.code)
        except ResponseSheetError as exc:
            raise SubmitFailed(f"{exc} Foto belum dikirim; coba lagi sebentar.") from exc
        status = STATUS_AGAIN if prior else STATUS_FIRST

        submission_id = uuid.uuid4().hex
        at = self.clock()
        try:
            stored = self.evidence.upload(photo, content_type=info.content_type, extension=info.extension,
                                          asm=store.asm, store_code=store.code, store_name=store.name, spv=spv,
                                          submitted_at=at, submission_id=submission_id)
        except EvidenceError as exc:
            raise SubmitFailed(str(exc)) from exc
        record = {
            "Timestamp": fmt_wib(at),
            "Nama ASM": store.asm,
            "KODE ST TOKO": store.code,
            "NAMA TOKO": store.name,
            "Region": store.region,
            "NAMA SPV": spv,
            "Ach SKU (MOQ)": fmt_number(store.ach),
            "Nama File": stored.file_name,
            "GCS Path": stored.gs_uri,
            "Status": status,
            "Waktu Foto (EXIF)": info.taken_at,
            "Lokasi Foto (EXIF)": info.gps,
            "Submission ID": submission_id,
        }
        try:
            self.responses.append(record)
        except ResponseSheetError as exc:
            # Foto dibiarkan di bucket (aplikasi tidak pernah menghapus); dicatat agar bisa dilacak.
            log.error("Foto tersimpan tetapi baris gagal ditulis: %s (%s)", stored.gs_uri, exc)
            raise SubmitFailed(f"{exc} Foto sudah tersimpan, tetapi data belum tercatat — silakan submit lagi.",
                               photo_saved=True) from exc
        log.info("Submit OK %s %s %s", submission_id, store.code, stored.gs_uri)
        return SubmitResult(submission_id=submission_id, submitted_at=record["Timestamp"], asm=store.asm,
                            store=store, spv=spv, photo=stored, info=info)
