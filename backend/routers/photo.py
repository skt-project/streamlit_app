"""
POST /photo/upload   — multipart or base64 upload to GCS
POST /photo/upload-b64 — base64 variant for offline sync payloads
"""
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from pydantic import BaseModel

from dependencies import require_auth
from models.auth import UserContext

router = APIRouter(prefix="/photo", tags=["photo"])


class PhotoUploadResponse(BaseModel):
    url: str
    file_id: str


class B64UploadRequest(BaseModel):
    visit_id: str
    photo_type: str   # checkin | checkout
    b64_data: str     # data:image/jpeg;base64,....


@router.post("/upload", response_model=PhotoUploadResponse)
async def upload_photo(
    visit_id: str = Form(...),
    photo_type: str = Form(...),
    file: UploadFile = File(...),
    current_user: UserContext = Depends(require_auth),
):
    try:
        from services.storage import upload_photo as _upload
        content = await file.read()
        url = _upload(content, file.content_type or "image/jpeg", visit_id, photo_type)
        return PhotoUploadResponse(url=url, file_id=f"{visit_id}_{photo_type}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")


@router.post("/upload-b64", response_model=PhotoUploadResponse)
def upload_photo_b64(
    body: B64UploadRequest,
    current_user: UserContext = Depends(require_auth),
):
    try:
        from services.storage import upload_photo_base64 as _upload
        url = _upload(body.b64_data, body.visit_id, body.photo_type)
        return PhotoUploadResponse(url=url, file_id=f"{body.visit_id}_{body.photo_type}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")
