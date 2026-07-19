import json

import redis as redis_lib
import structlog

from app.config import get_settings

logger = structlog.get_logger(__name__)
settings = get_settings()

DLQ_MAX_SIZE = settings.DLQ_MAX_SIZE
DLQ_TTL = settings.DLQ_TTL_SECONDS


def _get_redis() -> redis_lib.Redis:
    return redis_lib.from_url(settings.REDIS_URL, decode_responses=True)


def push_to_dlq(job_id: str, task_name: str, error: str, payload: dict, retry_count: int):
    r = _get_redis()
    try:
        entry = json.dumps({
            "job_id": job_id,
            "task_name": task_name,
            "error": error,
            "payload": payload,
            "retry_count": retry_count,
        })
        r.lpush("dead_letter_queue", entry)
        r.expire("dead_letter_queue", DLQ_TTL)
        current_size = r.llen("dead_letter_queue")
        if current_size > DLQ_MAX_SIZE:
            r.rpop("dead_letter_queue")
        logger.info("pushed_to_dlq", job_id=job_id, task_name=task_name)
    except Exception:
        logger.exception("push_to_dlq_failed", job_id=job_id)
    finally:
        r.close()


def pop_from_dlq(count: int = 1) -> list[dict]:
    r = _get_redis()
    try:
        entries = []
        for _ in range(count):
            raw = r.rpop("dead_letter_queue")
            if raw:
                entries.append(json.loads(raw))
            else:
                break
        return entries
    except Exception:
        logger.exception("pop_from_dlq_failed")
        return []
    finally:
        r.close()


def get_dlq_items(offset: int = 0, limit: int = 20) -> list[dict]:
    r = _get_redis()
    try:
        raw_items = r.lrange("dead_letter_queue", offset, offset + limit - 1)
        return [json.loads(item) for item in raw_items]
    except Exception:
        logger.exception("get_dlq_items_failed")
        return []
    finally:
        r.close()


def get_dlq_size() -> int:
    r = _get_redis()
    try:
        return r.llen("dead_letter_queue")
    except Exception:
        logger.exception("get_dlq_size_failed")
        return 0
    finally:
        r.close()


def clear_dlq():
    r = _get_redis()
    try:
        r.delete("dead_letter_queue")
        logger.info("dlq_cleared")
    except Exception:
        logger.exception("clear_dlq_failed")
    finally:
        r.close()
