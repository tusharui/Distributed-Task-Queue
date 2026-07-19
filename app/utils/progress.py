import json
import time

import redis as redis_lib
import structlog

from app.config import get_settings

logger = structlog.get_logger(__name__)
settings = get_settings()

PROGRESS_TTL = settings.PROGRESS_TTL_SECONDS


def _get_redis() -> redis_lib.Redis:
    return redis_lib.from_url(settings.REDIS_URL, decode_responses=True)


def set_progress(job_id: str, progress: int, stage: str = "", metadata: dict | None = None):
    r = _get_redis()
    try:
        key = f"job_progress:{job_id}"
        data = {
            "progress": progress,
            "stage": stage,
            "updated_at": time.time(),
        }
        if metadata:
            data["metadata"] = metadata
        r.set(key, json.dumps(data), ex=PROGRESS_TTL)
        r.publish(f"job_progress:{job_id}:updates", json.dumps(data))
    except Exception:
        logger.exception("set_progress_failed", job_id=job_id)
    finally:
        r.close()


def get_progress(job_id: str) -> dict | None:
    r = _get_redis()
    try:
        key = f"job_progress:{job_id}"
        raw = r.get(key)
        if raw:
            return json.loads(raw)
        return None
    except Exception:
        logger.exception("get_progress_failed", job_id=job_id)
        return None
    finally:
        r.close()


def delete_progress(job_id: str):
    r = _get_redis()
    try:
        r.delete(f"job_progress:{job_id}")
    except Exception:
        logger.exception("delete_progress_failed", job_id=job_id)
    finally:
        r.close()
