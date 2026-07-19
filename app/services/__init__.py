import uuid

import redis as redis_lib
import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.models import Job, JobStatus, JobType, Worker
from app.schemas import JobCreate
from app.tasks import TASK_MAP

logger = structlog.get_logger(__name__)
settings = get_settings()

PRIORITY_QUEUE_MAP = {
    range(7, 11): "high",
    range(3, 7): "default",
    range(0, 3): "low",
}


def _get_queue_for_priority(priority: int) -> str:
    for priority_range, queue in PRIORITY_QUEUE_MAP.items():
        if priority in priority_range:
            return queue
    return "default"


async def create_job(db: AsyncSession, job_data: JobCreate) -> Job:
    queue = _get_queue_for_priority(job_data.priority)

    job = Job(
        type=job_data.type,
        status=JobStatus.QUEUED,
        payload=job_data.payload,
        priority=job_data.priority,
        max_retries=job_data.max_retries,
    )
    db.add(job)
    await db.flush()

    task_func = TASK_MAP.get(job_data.type.value)
    if task_func:
        result = task_func.apply_async(
            args=[str(job.id), job_data.payload],
            queue=queue,
        )
        job.celery_task_id = result.id
        logger.info("job_dispatched", job_id=str(job.id), task_type=job_data.type.value, queue=queue)

    await db.flush()
    await db.refresh(job)
    return job


async def get_job(db: AsyncSession, job_id: uuid.UUID) -> Job | None:
    return await db.get(Job, job_id)


async def list_jobs(
    db: AsyncSession,
    status: JobStatus | None = None,
    job_type: JobType | None = None,
    offset: int = 0,
    limit: int = 20,
) -> tuple[list[Job], int]:
    query = select(Job)
    count_query = select(func.count(Job.id))

    if status:
        query = query.where(Job.status == status)
        count_query = count_query.where(Job.status == status)
    if job_type:
        query = query.where(Job.type == job_type)
        count_query = count_query.where(Job.type == job_type)

    total = (await db.execute(count_query)).scalar() or 0
    query = query.order_by(Job.created_at.desc()).offset(offset).limit(limit)
    result = await db.execute(query)
    return list(result.scalars().all()), total


async def cancel_job(db: AsyncSession, job_id: uuid.UUID) -> tuple[Job | None, str]:
    job = await db.get(Job, job_id)
    if not job:
        return None, "not_found"

    if job.status in (JobStatus.COMPLETED, JobStatus.CANCELLED):
        return job, "already_terminal"

    if job.status == JobStatus.FAILED:
        return job, "already_failed"

    if job.celery_task_id:
        from app.tasks.celery_app import celery_app

        celery_app.control.revoke(job.celery_task_id, terminate=True)

    job.status = JobStatus.CANCELLED
    await db.flush()
    logger.info("job_cancelled", job_id=str(job_id))
    return job, "cancelled"


async def retry_job(db: AsyncSession, job_id: uuid.UUID) -> tuple[Job | None, str]:
    job = await db.get(Job, job_id)
    if not job:
        return None, "not_found"

    if job.status != JobStatus.FAILED:
        return job, "not_failed"

    if job.retry_count >= job.max_retries:
        return job, "max_retries_exceeded"

    queue = _get_queue_for_priority(job.priority)

    job.status = JobStatus.QUEUED
    job.retry_count += 1
    job.error = None
    job.result = None

    task_func = TASK_MAP.get(job.type.value)
    if task_func:
        result = task_func.apply_async(
            args=[str(job.id), job.payload],
            queue=queue,
        )
        job.celery_task_id = result.id
        logger.info("job_retried", job_id=str(job_id), retry_count=job.retry_count, queue=queue)

    await db.flush()
    await db.refresh(job)
    return job, "retried"


async def get_worker(db: AsyncSession, worker_id: uuid.UUID) -> Worker | None:
    return await db.get(Worker, worker_id)


async def get_workers(db: AsyncSession) -> list[Worker]:
    result = await db.execute(select(Worker).order_by(Worker.registered_at.desc()))
    return list(result.scalars().all())


async def get_queue_depths() -> dict[str, int]:
    try:
        r = redis_lib.from_url(settings.CELERY_BROKER_URL)
        queues = {}
        for queue_name in ("high", "default", "low"):
            depth = r.llen(queue_name)
            queues[queue_name] = depth
        r.close()
        return queues
    except Exception:
        logger.exception("queue_depth_check_failed")
        return {"high": 0, "default": 0, "low": 0}


async def get_stats(db: AsyncSession) -> dict:
    total = (await db.execute(select(func.count(Job.id)))).scalar() or 0

    status_counts = {}
    for status in JobStatus:
        count = (
            await db.execute(
                select(func.count(Job.id)).where(Job.status == status)
            )
        ).scalar() or 0
        status_counts[status.value] = count

    type_counts = {}
    for job_type in JobType:
        count = (
            await db.execute(
                select(func.count(Job.id)).where(Job.type == job_type)
            )
        ).scalar() or 0
        type_counts[job_type.value] = count

    active_workers = (
        await db.execute(
            select(func.count(Worker.id)).where(Worker.status == "active")
        )
    ).scalar() or 0

    queue_depth = (
        await db.execute(
            select(func.count(Job.id)).where(
                Job.status.in_([JobStatus.PENDING, JobStatus.QUEUED])
            )
        )
    ).scalar() or 0

    avg_processing_time = None
    completed_jobs = (
        await db.execute(
            select(Job.started_at, Job.completed_at).where(
                Job.status == JobStatus.COMPLETED,
                Job.started_at.isnot(None),
                Job.completed_at.isnot(None),
            )
        )
    ).all()
    if completed_jobs:
        durations = [
            (row.completed_at - row.started_at).total_seconds()
            for row in completed_jobs
            if row.completed_at and row.started_at
        ]
        if durations:
            avg_processing_time = round(sum(durations) / len(durations), 2)

    error_rate = 0.0
    failed_count = status_counts.get("failed", 0)
    if total > 0:
        error_rate = round((failed_count / total) * 100, 2)

    queues = await get_queue_depths()

    return {
        "total_jobs": total,
        "jobs_by_status": status_counts,
        "jobs_by_type": type_counts,
        "active_workers": active_workers,
        "queue_depth": queue_depth,
        "avg_processing_time_seconds": avg_processing_time,
        "error_rate_percent": error_rate,
        "queues": queues,
    }
