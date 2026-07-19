import structlog
from fastapi import APIRouter

from app.schemas import ProgressResponse
from app.utils.progress import get_progress

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/progress", tags=["progress"])


@router.get("/{job_id}", response_model=ProgressResponse)
async def get_job_progress(job_id: str):
    progress = get_progress(job_id)
    if not progress:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Progress not found or expired")
    return ProgressResponse(
        job_id=job_id,
        progress=progress["progress"],
        stage=progress.get("stage", ""),
        updated_at=progress.get("updated_at", 0),
    )
