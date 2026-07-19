import uuid

import structlog
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import Job, JobStatus
from app.schemas import PipelineCreate, PipelineResponse
from app.services import _get_queue_for_priority

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/pipelines", tags=["pipelines"])


@router.post("", response_model=PipelineResponse, status_code=201)
async def create_pipeline(
    pipeline_data: PipelineCreate,
    db: AsyncSession = Depends(get_db),
):
    from app.tasks import TASK_MAP

    pipeline_id = uuid.uuid4()
    job_ids = []

    for idx, step in enumerate(pipeline_data.steps):
        queue = _get_queue_for_priority(step.priority)
        job = Job(
            id=uuid.uuid4() if idx > 0 else pipeline_id,
            type=step.type,
            status=JobStatus.QUEUED,
            payload=step.payload,
            priority=step.priority,
            max_retries=step.max_retries,
        )
        if idx == 0:
            pipeline_id = job.id
        db.add(job)
        await db.flush()
        job_ids.append(job.id)

        task_func = TASK_MAP.get(step.type.value)
        if task_func:
            result = task_func.apply_async(
                args=[str(job.id), step.payload],
                queue=queue,
            )
            job.celery_task_id = result.id
            logger.info(
                "pipeline_step_dispatched",
                pipeline_id=str(pipeline_id),
                step=idx,
                job_id=str(job.id),
                queue=queue,
            )

    await db.flush()

    return PipelineResponse(
        pipeline_id=pipeline_id,
        job_ids=job_ids,
        status="dispatched",
        message=f"Pipeline '{pipeline_data.name or 'unnamed'}' dispatched with {len(job_ids)} steps",
    )


@router.get("/{pipeline_id}/status")
async def get_pipeline_status(
    pipeline_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Job).where(Job.id == pipeline_id)
    )
    pipeline_job = result.scalar_one_or_none()
    if not pipeline_job:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    query = select(Job).where(
        Job.created_at >= pipeline_job.created_at
    ).order_by(Job.created_at.asc())
    jobs_result = await db.execute(query)
    pipeline_jobs = list(jobs_result.scalars().all())

    job_statuses = [
        {
            "job_id": str(j.id),
            "type": j.type.value,
            "status": j.status.value,
            "progress": j.progress,
        }
        for j in pipeline_jobs[:10]
    ]

    statuses = [j.status.value for j in pipeline_jobs[:10]]
    if all(s == "completed" for s in statuses):
        overall_status = "completed"
    elif any(s == "failed" for s in statuses):
        overall_status = "failed"
    elif any(s in ("running", "queued") for s in statuses):
        overall_status = "running"
    else:
        overall_status = "pending"

    return {
        "pipeline_id": str(pipeline_id),
        "overall_status": overall_status,
        "steps": job_statuses,
    }
