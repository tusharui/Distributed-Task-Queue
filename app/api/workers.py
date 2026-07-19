import uuid

import structlog
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.schemas import MessageResponse, WorkerListResponse, WorkerRegisterRequest, WorkerResponse
from app.services import get_worker, get_workers

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/workers", tags=["workers"])


@router.get("", response_model=WorkerListResponse)
async def list_workers_endpoint(
    db: AsyncSession = Depends(get_db),
):
    workers = await get_workers(db)
    active = sum(1 for w in workers if w.status == "active")
    inactive = sum(1 for w in workers if w.status != "active")
    return WorkerListResponse(
        workers=workers,
        total=len(workers),
        active_count=active,
        inactive_count=inactive,
    )


@router.get("/{worker_id}", response_model=WorkerResponse)
async def get_worker_endpoint(
    worker_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    worker = await get_worker(db, worker_id)
    if not worker:
        raise HTTPException(status_code=404, detail="Worker not found")
    return worker


@router.post("/register", response_model=MessageResponse, status_code=201)
async def register_worker_endpoint(
    request: WorkerRegisterRequest,
    db: AsyncSession = Depends(get_db),
):
    from app.tasks.worker_tasks import register_worker

    result = register_worker.delay(request.hostname, request.pid)
    logger.info("worker_register_requested", hostname=request.hostname, pid=request.pid)
    return MessageResponse(message=f"Worker registration initiated (task: {result.id})")


@router.post("/{worker_id}/shutdown", response_model=MessageResponse)
async def shutdown_worker_endpoint(
    worker_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    worker = await get_worker(db, worker_id)
    if not worker:
        raise HTTPException(status_code=404, detail="Worker not found")

    if worker.status != "active":
        raise HTTPException(
            status_code=409,
            detail=f"Worker is '{worker.status}', cannot shutdown",
        )

    try:
        from app.tasks.celery_app import celery_app

        celery_app.control.broadcast("shutdown")
        logger.info("worker_shutdown_broadcast", worker_id=str(worker_id))
        return MessageResponse(message=f"Shutdown signal sent to worker {worker_id}")
    except Exception:
        logger.exception("worker_shutdown_failed", worker_id=str(worker_id))
        raise HTTPException(status_code=500, detail="Failed to send shutdown signal")
