import uuid
from pathlib import Path

import structlog
from fastapi import UploadFile

from app.config import get_settings

logger = structlog.get_logger(__name__)
settings = get_settings()


def _ensure_upload_dir():
    upload_dir = Path(settings.UPLOAD_DIR)
    upload_dir.mkdir(parents=True, exist_ok=True)
    return upload_dir


async def save_uploaded_file(file: UploadFile) -> tuple[str, str, int]:
    file_id = str(uuid.uuid4())
    original_name = file.filename or "unknown"
    ext = Path(original_name).suffix
    safe_filename = f"{file_id}{ext}"

    upload_dir = _ensure_upload_dir()
    file_path = upload_dir / safe_filename

    content = await file.read()
    file_size = len(content)

    max_size = settings.MAX_FILE_SIZE_MB * 1024 * 1024
    if file_size > max_size:
        raise ValueError(f"File size {file_size} exceeds maximum {settings.MAX_FILE_SIZE_MB}MB")

    with open(file_path, "wb") as f:
        f.write(content)

    logger.info("file_uploaded", file_id=file_id, filename=original_name, size=file_size)
    return file_id, str(file_path), file_size


def get_file_path(file_id: str) -> str | None:
    upload_dir = Path(settings.UPLOAD_DIR)
    for f in upload_dir.iterdir():
        if f.stem == file_id:
            return str(f)
    return None


def delete_file(file_path: str) -> bool:
    try:
        path = Path(file_path)
        if path.exists():
            path.unlink()
            logger.info("file_deleted", file_path=file_path)
            return True
    except Exception:
        logger.exception("file_delete_failed", file_path=file_path)
    return False
