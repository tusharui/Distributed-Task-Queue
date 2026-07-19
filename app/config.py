from functools import lru_cache

import structlog
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # PostgreSQL
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = ""
    POSTGRES_DB: str = "taskqueue"
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/taskqueue"

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"

    # Celery
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/0"
    CELERY_SOFT_TIME_LIMIT: int = 300
    CELERY_TIME_LIMIT: int = 600
    CELERY_RESULT_EXPIRES: int = 3600
    CELERY_MAX_PRIORITY: int = 10

    # API
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    APP_VERSION: str = "0.2.0"
    CORS_ORIGINS: list[str] = ["*"]

    # Logging
    LOG_LEVEL: str = "INFO"

    # File Upload
    UPLOAD_DIR: str = "uploads"
    MAX_FILE_SIZE_MB: int = 50

    # Database Pool
    DB_POOL_SIZE: int = 20
    DB_MAX_OVERFLOW: int = 10

    # Worker Heartbeat / Cleanup
    HEARTBEAT_INTERVAL: int = 30
    STALE_WORKER_CLEANUP_INTERVAL: int = 60
    STUCK_JOB_RECOVERY_INTERVAL: int = 120
    STALE_WORKER_TIMEOUT: int = 90
    STUCK_JOB_TIMEOUT: int = 600

    # Circuit Breaker
    CB_DEFAULT_FAILURE_THRESHOLD: int = 5
    CB_DEFAULT_RECOVERY_TIMEOUT: int = 60

    # Dead Letter Queue
    DLQ_MAX_SIZE: int = 1000
    DLQ_TTL_SECONDS: int = 604800  # 7 days

    # Progress Tracking
    PROGRESS_TTL_SECONDS: int = 3600

    # Health Check
    HEALTH_CHECK_TIMEOUT: float = 2.0

    # Retry Backoff
    RETRY_BACKOFF_BASE: int = 10
    RETRY_BACKOFF_JITTER_MAX: int = 30

    # Pagination
    DEFAULT_PAGE_SIZE: int = 20
    MAX_PAGE_SIZE: int = 100

    model_config = {"env_file": ".env", "extra": "ignore"}


@lru_cache
def get_settings() -> Settings:
    return Settings()


def setup_logging(log_level: str | None = None):
    settings = get_settings()
    level = log_level or settings.LOG_LEVEL

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    import logging
    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, level.upper(), logging.INFO),
    )
