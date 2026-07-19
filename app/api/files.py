
import structlog
from fastapi import APIRouter, Depends, HTTPException, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import Job, JobStatus, JobType
from app.schemas import FileUploadResponse, JobCreate, MessageResponse
from app.services.file_storage import delete_file, get_file_path, save_uploaded_file

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/files", tags=["files"])


@router.post("/upload", response_model=FileUploadResponse, status_code=201)
async def upload_file(
    file: UploadFile,
    job_type: JobType = JobType.CUSTOM,
    priority: int = 0,
    max_retries: int = 3,
    db: AsyncSession = Depends(get_db),
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")

    try:
        file_id, file_path, file_size = await save_uploaded_file(file)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("file_upload_failed", filename=file.filename)
        raise HTTPException(status_code=500, detail="Failed to save file")

    job_data = JobCreate(
        type=job_type,
        payload={"file_id": file_id, "file_path": file_path, "filename": file.filename},
        priority=priority,
        max_retries=max_retries,
    )

    job = Job(
        type=job_type,
        status=JobStatus.QUEUED,
        payload=job_data.payload,
        priority=priority,
        max_retries=max_retries,
        file_path=file_path,
        file_name=file.filename,
        file_size=file_size,
    )
    db.add(job)
    await db.flush()

    from app.tasks import TASK_MAP

    task_func = TASK_MAP.get(job_type.value)
    if task_func:
        result = task_func.delay(str(job.id), job_data.payload)
        job.celery_task_id = result.id
        logger.info("file_job_dispatched", job_id=str(job.id), job_type=job_type.value)

    await db.flush()

    logger.info("file_uploaded_job_created", job_id=str(job.id), file_id=file_id)
    return FileUploadResponse(
        file_id=file_id,
        file_name=file.filename,
        file_size=file_size,
        message=f"File uploaded and job {job.id} created",
    )


@router.get("/{file_id}/download")
async def download_file(file_id: str):
    file_path = get_file_path(file_id)
    if not file_path:
        raise HTTPException(status_code=404, detail="File not found")

    from fastapi.responses import FileResponse

    return FileResponse(file_path, filename=file_id)


@router.delete("/{file_id}", response_model=MessageResponse)
async def delete_file_endpoint(file_id: str):
    file_path = get_file_path(file_id)
    if not file_path:
        raise HTTPException(status_code=404, detail="File not found")

    success = delete_file(file_path)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete file")

    return MessageResponse(message=f"File {file_id} deleted")
