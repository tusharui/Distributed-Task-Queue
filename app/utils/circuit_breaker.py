import time
from enum import Enum

import redis as redis_lib
import structlog

from app.config import get_settings

logger = structlog.get_logger(__name__)
settings = get_settings()

DEFAULT_FAILURE_THRESHOLD = settings.CB_DEFAULT_FAILURE_THRESHOLD
DEFAULT_RECOVERY_TIMEOUT = settings.CB_DEFAULT_RECOVERY_TIMEOUT


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self,
        name: str,
        failure_threshold: int = DEFAULT_FAILURE_THRESHOLD,
        recovery_timeout: int = DEFAULT_RECOVERY_TIMEOUT,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

    def _get_redis(self) -> redis_lib.Redis:
        return redis_lib.from_url(settings.REDIS_URL, decode_responses=True)

    def _key(self, field: str) -> str:
        return f"circuit_breaker:{self.name}:{field}"

    def record_success(self):
        r = self._get_redis()
        try:
            pipe = r.pipeline()
            pipe.set(self._key("failures"), 0)
            pipe.set(self._key("state"), CircuitState.CLOSED.value)
            pipe.set(self._key("last_failure"), "")
            pipe.execute()
        except Exception:
            logger.exception("circuit_breaker_record_success_failed", name=self.name)
        finally:
            r.close()

    def record_failure(self):
        r = self._get_redis()
        try:
            pipe = r.pipeline()
            failures = r.incr(self._key("failures"))
            pipe.set(self._key("last_failure"), str(time.time()))
            if failures >= self.failure_threshold:
                pipe.set(self._key("state"), CircuitState.OPEN.value)
                logger.warning(
                    "circuit_breaker_opened",
                    name=self.name,
                    failures=failures,
                )
            pipe.execute()
        except Exception:
            logger.exception("circuit_breaker_record_failure_failed", name=self.name)
        finally:
            r.close()

    def allow_request(self) -> bool:
        r = self._get_redis()
        try:
            state = r.get(self._key("state")) or CircuitState.CLOSED.value

            if state == CircuitState.CLOSED.value:
                return True

            if state == CircuitState.OPEN.value:
                last_failure = r.get(self._key("last_failure"))
                if last_failure and (time.time() - float(last_failure)) > self.recovery_timeout:
                    r.set(self._key("state"), CircuitState.HALF_OPEN.value)
                    logger.info("circuit_breaker_half_open", name=self.name)
                    return True
                return False

            if state == CircuitState.HALF_OPEN.value:
                return True

            return True
        except Exception:
            logger.exception("circuit_breaker_check_failed", name=self.name)
            return True
        finally:
            r.close()

    def get_state(self) -> dict:
        r = self._get_redis()
        try:
            return {
                "name": self.name,
                "state": r.get(self._key("state")) or CircuitState.CLOSED.value,
                "failures": int(r.get(self._key("failures")) or 0),
                "failure_threshold": self.failure_threshold,
                "recovery_timeout": self.recovery_timeout,
                "last_failure": r.get(self._key("last_failure")) or None,
            }
        except Exception:
            return {
                "name": self.name,
                "state": CircuitState.CLOSED.value,
                "failures": 0,
                "failure_threshold": self.failure_threshold,
                "recovery_timeout": self.recovery_timeout,
                "last_failure": None,
            }
        finally:
            r.close()


_task_breakers: dict[str, CircuitBreaker] = {}

_breaker_configs = {
    "pdf_summarization": {"failure_threshold": 3, "recovery_timeout": 120},
    "ai_text_generation": {"failure_threshold": 5, "recovery_timeout": 60},
    "html_to_text": {"failure_threshold": 5, "recovery_timeout": 60},
    "image_resizing": {"failure_threshold": 5, "recovery_timeout": 60},
    "csv_processing": {"failure_threshold": 5, "recovery_timeout": 60},
    "data_validation": {"failure_threshold": 5, "recovery_timeout": 60},
    "file_conversion": {"failure_threshold": 5, "recovery_timeout": 60},
    "custom": {"failure_threshold": 5, "recovery_timeout": 60},
}


def get_breaker(task_name: str) -> CircuitBreaker:
    if task_name not in _task_breakers:
        config = _breaker_configs.get(task_name, {})
        _task_breakers[task_name] = CircuitBreaker(
            name=task_name,
            failure_threshold=config.get("failure_threshold", DEFAULT_FAILURE_THRESHOLD),
            recovery_timeout=config.get("recovery_timeout", DEFAULT_RECOVERY_TIMEOUT),
        )
    return _task_breakers[task_name]


def get_all_breaker_states() -> list[dict]:
    return [get_breaker(name).get_state() for name in _breaker_configs]
