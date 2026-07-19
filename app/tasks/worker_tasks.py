import os
import socket
import uuid
from datetime import UTC, datetime, timedelta

import structlog
from celery import shared_task
from sqlalchemy import create_engine, update
from sqlalchemy.orm import sessionmaker

from app.config import get_settings
from app.models import Job, JobStatus, Worker

logger = structlog.get_logger(__name__)
settings = get_settings()

sync_database_url = settings.DATABASE_URL.replace("+asyncpg", "+psycopg2")
engine = create_engine(sync_database_url, pool_size=2, max_overflow=2)
SyncSession = sessionmaker(bind=engine)

STUCK_JOB_TIMEOUT = settings.STUCK_JOB_TIMEOUT


@shared_task(name="app.tasks.worker_tasks.recover_stuck_jobs")
def recover_stuck_jobs():
    session = SyncSession()
    try:
        cutoff = datetime.now(UTC) - timedelta(seconds=STUCK_JOB_TIMEOUT)
        result = session.execute(
            update(Job)
            .where(
                Job.status == JobStatus.RUNNING,
                Job.started_at < cutoff,
            )
            .values(
                status=JobStatus.FAILED,
                error=f"Job timed out after {STUCK_JOB_TIMEOUT}s - marked as stuck",
                completed_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
        )
        if result.rowcount > 0:
            session.commit()
            logger.warning("stuck_jobs_recovered", count=result.rowcount)
        else:
            session.commit()
    except Exception:
        session.rollback()
        logger.exception("recover_stuck_jobs_failed")
    finally:
        session.close()


@shared_task(name="app.tasks.worker_tasks.worker_heartbeat")
def worker_heartbeat():
    hostname = socket.gethostname()
    pid = os.getpid()

    session = SyncSession()
    try:
        worker = (
            session.query(Worker)
            .filter(Worker.hostname == hostname, Worker.pid == pid)
            .first()
        )
        if worker:
            worker.last_heartbeat = datetime.now(UTC)
            worker.status = "active"
        else:
            worker = Worker(
                id=uuid.uuid4(),
                hostname=hostname,
                pid=pid,
                status="active",
                registered_at=datetime.now(UTC),
                last_heartbeat=datetime.now(UTC),
            )
            session.add(worker)
        session.commit()
        logger.info("worker_heartbeat", hostname=hostname, pid=pid)
    except Exception:
        session.rollback()
        logger.exception("worker_heartbeat_failed", hostname=hostname, pid=pid)
    finally:
        session.close()


@shared_task(name="app.tasks.worker_tasks.cleanup_stale_workers")
def cleanup_stale_workers():
    session = SyncSession()
    try:
        cutoff = datetime.now(UTC) - timedelta(seconds=settings.STALE_WORKER_TIMEOUT)
        result = session.execute(
            update(Worker)
            .where(Worker.status == "active", Worker.last_heartbeat < cutoff)
            .values(status="inactive")
        )
        if result.rowcount > 0:
            session.commit()
            logger.info("stale_workers_cleaned", count=result.rowcount)
        else:
            session.commit()
    except Exception:
        session.rollback()
        logger.exception("cleanup_stale_workers_failed")
    finally:
        session.close()


@shared_task(name="app.tasks.worker_tasks.register_worker")
def register_worker(hostname: str, pid: int):
    session = SyncSession()
    try:
        worker = Worker(
            id=uuid.uuid4(),
            hostname=hostname,
            pid=pid,
            status="active",
            registered_at=datetime.now(UTC),
            last_heartbeat=datetime.now(UTC),
        )
        session.add(worker)
        session.commit()
        logger.info("worker_registered", hostname=hostname, pid=pid, worker_id=str(worker.id))
        return {"worker_id": str(worker.id), "status": "registered"}
    except Exception:
        session.rollback()
        logger.exception("register_worker_failed", hostname=hostname, pid=pid)
        return {"status": "error"}
    finally:
        session.close()
