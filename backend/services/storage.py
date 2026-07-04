"""Google Cloud Storage helper for photo uploads."""
import base64
import io
import json
import os
import uuid
from datetime import datetime, timezone

from config import settings


def _get_gcs_client():
    from google.cloud import storage as gcs
    from google.oauth2 import service_account

    scopes = ["https://www.googleapis.com/auth/devstorage.read_write"]
    if settings.bq_sa_key_json:
        info = json.loads(base64.b64decode(settings.bq_sa_key_json).decode())
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    elif settings.bq_sa_key_path:
        creds = service_account.Credentials.from_service_account_file(
            settings.bq_sa_key_path, scopes=scopes
        )
    else:
        creds = None
    return gcs.Client(project=settings.bq_project, credentials=creds)


GCS_BUCKET = os.getenv("GCS_BUCKET", "sfa-portal-photos")


def upload_photo(
    file_bytes: bytes,
    content_type: str,
    visit_id: str,
    photo_type: str,  # "checkin" | "checkout"
) -> str:
    """Upload photo to GCS, return public URL."""
    client = _get_gcs_client()
    bucket = client.bucket(GCS_BUCKET)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    ext = "jpg" if "jpeg" in content_type.lower() or "jpg" in content_type.lower() else "png"
    blob_name = f"visits/{photo_type}/{visit_id}_{ts}_{uuid.uuid4().hex[:8]}.{ext}"

    blob = bucket.blob(blob_name)
    blob.upload_from_file(io.BytesIO(file_bytes), content_type=content_type)
    blob.make_public()
    return blob.public_url


def upload_photo_base64(b64_data: str, visit_id: str, photo_type: str) -> str:
    """Upload from base64-encoded string (used by offline sync payloads)."""
    if "," in b64_data:
        header, data = b64_data.split(",", 1)
        content_type = header.split(":")[1].split(";")[0] if ":" in header else "image/jpeg"
    else:
        data, content_type = b64_data, "image/jpeg"
    return upload_photo(base64.b64decode(data), content_type, visit_id, photo_type)
