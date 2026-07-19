import os
import time

import redis as redis_lib
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.database import get_db
from app.utils.circuit_breaker import get_all_breaker_states
from app.utils.dead_letter_queue import get_dlq_size

router = APIRouter()
settings = get_settings()

_start_time = time.time()


@router.get("/health")
async def health_check(db: AsyncSession = Depends(get_db)):
    checks = {}

    try:
        await db.execute(text("SELECT 1"))
        checks["postgres"] = "healthy"
    except Exception as e:
        checks["postgres"] = f"unhealthy: {str(e)}"

    try:
        from app.tasks.celery_app import celery_app

        inspect = celery_app.control.inspect(timeout=settings.HEALTH_CHECK_TIMEOUT)
        ping = inspect.ping()
        if ping:
            worker_count = len(ping)
            checks["celery"] = f"healthy ({worker_count} workers)"
        else:
            checks["celery"] = "unhealthy: no workers responding"
    except Exception as e:
        checks["celery"] = f"unhealthy: {str(e)}"

    try:
        redis_url = os.environ.get("CELERY_BROKER_URL", settings.REDIS_URL)
        r = redis_lib.from_url(redis_url, socket_timeout=settings.HEALTH_CHECK_TIMEOUT)
        r.ping()
        r.close()
        checks["redis"] = "healthy"
    except Exception as e:
        checks["redis"] = f"unhealthy: {str(e)}"

    breakers = get_all_breaker_states()
    open_breakers = [b for b in breakers if b["state"] != "closed"]
    if open_breakers:
        checks["circuit_breakers"] = f"degraded ({len(open_breakers)} open)"
    else:
        checks["circuit_breakers"] = "healthy"

    dlq_size = get_dlq_size()
    if dlq_size > 0:
        checks["dead_letter_queue"] = f"active ({dlq_size} items)"
    else:
        checks["dead_letter_queue"] = "empty"

    overall = "healthy"
    for key, value in checks.items():
        if key.startswith("circuit_breakers") or key.startswith("dead_letter_queue"):
            continue
        if value != "healthy" and not value.startswith("healthy"):
            overall = "degraded"
            break

    return {
        "status": overall,
        "checks": checks,
        "uptime_seconds": round(time.time() - _start_time, 2),
    }
