import uuid
from datetime import UTC, datetime

import structlog
from celery import chord, group, shared_task
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.config import get_settings
from app.models import Job, JobStatus
from app.utils.progress import set_progress

logger = structlog.get_logger(__name__)
settings = get_settings()

sync_database_url = settings.DATABASE_URL.replace("+asyncpg", "+psycopg2")
engine = create_engine(sync_database_url, pool_size=3, max_overflow=3)
SyncSession = sessionmaker(bind=engine)


def _update_workflow_job(job_id: uuid.UUID, status: JobStatus, result: dict | None = None, error: str | None = None):
    session = SyncSession()
    try:
        job = session.get(Job, job_id)
        if job:
            job.status = status
            job.updated_at = datetime.now(UTC)
            if status in (JobStatus.COMPLETED, JobStatus.FAILED):
                job.completed_at = datetime.now(UTC)
            if result is not None:
                job.result = result
            if error is not None:
                job.error = error
            session.commit()
    except Exception:
        session.rollback()
        logger.exception("workflow_job_update_failed", job_id=str(job_id))
    finally:
        session.close()


@shared_task(name="app.tasks.workflows.on_step_complete")
def on_step_complete(job_id: str, step_results: list):
    _update_workflow_job(
        uuid.UUID(job_id),
        JobStatus.COMPLETED,
        result={"steps": step_results, "total_steps": len(step_results)},
    )
    set_progress(job_id, 100, stage="pipeline_complete")
    logger.info("workflow_complete", job_id=job_id, steps=len(step_results))


@shared_task(name="app.tasks.workflows.on_step_failure")
def on_step_failure(job_id: str, exc: Exception):
    _update_workflow_job(
        uuid.UUID(job_id),
        JobStatus.FAILED,
        error=f"Pipeline step failed: {exc}",
    )
    set_progress(job_id, 0, stage="pipeline_failed")
    logger.error("workflow_failed", job_id=job_id, error=str(exc))


@shared_task(name="app.tasks.workflows.execute_parallel_group")
def execute_parallel_group(job_id: str, task_specs: list[dict]):
    from app.tasks import TASK_MAP

    job_uuid = uuid.UUID(job_id)
    _update_workflow_job(job_uuid, JobStatus.RUNNING)
    set_progress(job_id, 0, stage="parallel_start")

    tasks = []
    for spec in task_specs:
        task_name = spec.get("type", "custom")
        payload = spec.get("payload", {})
        task_func = TASK_MAP.get(task_name)
        if task_func:
            step_job_id = str(uuid.uuid4())
            tasks.append(task_func.s(step_job_id, payload))

    if not tasks:
        _update_workflow_job(job_uuid, JobStatus.FAILED, error="No valid tasks in group")
        return {"error": "no_valid_tasks"}

    workflow_id = str(uuid.uuid4())
    callback = on_step_complete.s(job_id)

    chord(group(tasks))(callback)
    set_progress(job_id, 10, stage="parallel_dispatched")
    logger.info("parallel_group_dispatched", job_id=job_id, task_count=len(tasks))
    return {"workflow_id": workflow_id, "tasks_dispatched": len(tasks)}
