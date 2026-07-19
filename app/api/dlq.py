import structlog
from fastapi import APIRouter, HTTPException, Query

from app.schemas import DLQItem, DLQResponse, MessageResponse
from app.utils.dead_letter_queue import clear_dlq, get_dlq_items, get_dlq_size

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/dlq", tags=["dead-letter-queue"])


@router.get("", response_model=DLQResponse)
async def list_dlq(
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
):
    items = get_dlq_items(offset=offset, limit=limit)
    total = get_dlq_size()
    return DLQResponse(
        items=[DLQItem(**item) for item in items],
        total=total,
    )


@router.post("/{job_id}/retry", response_model=MessageResponse)
async def retry_from_dlq(job_id: str):
    items = get_dlq_items(offset=0, limit=get_dlq_size())
    for item in items:
        if item["job_id"] == job_id:
            from app.tasks import TASK_MAP

            task_func = TASK_MAP.get(item["task_name"])
            if task_func:
                task_func.apply_async(
                    args=[item["job_id"], item["payload"]],
                    queue="default",
                )
                logger.info("dlq_job_retried", job_id=job_id, task_name=item["task_name"])
                return MessageResponse(message=f"Job {job_id} re-dispatched from DLQ")
            return MessageResponse(message=f"Task type '{item['task_name']}' not found")
    raise HTTPException(status_code=404, detail="Job not found in DLQ")


@router.delete("", response_model=MessageResponse)
async def clear_dlq_endpoint():
    clear_dlq()
    return MessageResponse(message="Dead letter queue cleared")
