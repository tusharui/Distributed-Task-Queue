import uuid

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import JobStatus, JobType
from app.schemas import JobCreate, JobListResponse, JobResponse, MessageResponse
from app.services import cancel_job, create_job, get_job, list_jobs, retry_job

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("", response_model=JobResponse, status_code=201)
async def create_job_endpoint(
    job_data: JobCreate,
    db: AsyncSession = Depends(get_db),
):
    job = await create_job(db, job_data)
    logger.info("job_created", job_id=str(job.id), job_type=job_data.type.value)
    return job


@router.get("", response_model=JobListResponse)
async def list_jobs_endpoint(
    status: JobStatus | None = Query(None, description="Filter by status"),
    job_type: JobType | None = Query(None, alias="type", description="Filter by type"),
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    jobs, total = await list_jobs(db, status=status, job_type=job_type, offset=offset, limit=limit)
    return JobListResponse(jobs=jobs, total=total)


@router.get("/{job_id}", response_model=JobResponse)
async def get_job_endpoint(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    job = await get_job(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.post("/{job_id}/cancel", response_model=MessageResponse)
async def cancel_job_endpoint(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    job, reason = await cancel_job(db, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if reason == "already_terminal":
        raise HTTPException(
            status_code=409,
            detail=f"Job is already in '{job.status.value}' status and cannot be cancelled",
        )
    if reason == "already_failed":
        raise HTTPException(
            status_code=409,
            detail="Job has already failed. Use POST /jobs/{id}/retry instead",
        )
    return MessageResponse(message=f"Job {job_id} cancelled")


@router.delete("/{job_id}", response_model=MessageResponse)
async def delete_job_endpoint(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    job, reason = await cancel_job(db, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if reason == "already_terminal":
        raise HTTPException(
            status_code=409,
            detail=f"Job is already in '{job.status.value}' status",
        )
    if reason == "already_failed":
        raise HTTPException(
            status_code=409,
            detail="Job has already failed. Use POST /jobs/{id}/retry instead",
        )
    return MessageResponse(message=f"Job {job_id} cancelled")


@router.post("/{job_id}/retry", response_model=JobResponse)
async def retry_job_endpoint(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    job, reason = await retry_job(db, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if reason == "not_failed":
        raise HTTPException(
            status_code=400,
            detail=f"Cannot retry job in '{job.status.value}' status. Only failed jobs can be retried",
        )
    if reason == "max_retries_exceeded":
        raise HTTPException(
            status_code=400,
            detail=f"Job has exceeded maximum retries ({job.max_retries}). Retry count: {job.retry_count}",
        )
    return job
