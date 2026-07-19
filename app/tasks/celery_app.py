from celery import Celery

from app.config import get_settings

settings = get_settings()

celery_app = Celery(
    "worker",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_default_queue="default",
    task_routes={
        "app.tasks.pdf_summarization": {"queue": "default"},
        "app.tasks.image_resizing": {"queue": "default"},
        "app.tasks.csv_processing": {"queue": "default"},
        "app.tasks.ai_text_generation": {"queue": "high"},
        "app.tasks.html_to_text": {"queue": "default"},
        "app.tasks.data_validation": {"queue": "default"},
        "app.tasks.file_conversion": {"queue": "default"},
        "app.tasks.custom_task": {"queue": "default"},
        "app.tasks.workflows.*": {"queue": "default"},
    },
    task_queue_max_priority=settings.CELERY_MAX_PRIORITY,
    worker_hijack_root_logger=False,
    task_soft_time_limit=settings.CELERY_SOFT_TIME_LIMIT,
    task_time_limit=settings.CELERY_TIME_LIMIT,
    result_expires=settings.CELERY_RESULT_EXPIRES,
    task_send_sent_event=True,
    worker_send_task_events=True,
    beat_schedule={
        "heartbeat": {
            "task": "app.tasks.worker_tasks.worker_heartbeat",
            "schedule": settings.HEARTBEAT_INTERVAL,
        },
        "cleanup-stale-workers": {
            "task": "app.tasks.worker_tasks.cleanup_stale_workers",
            "schedule": settings.STALE_WORKER_CLEANUP_INTERVAL,
        },
        "recover-stuck-jobs": {
            "task": "app.tasks.worker_tasks.recover_stuck_jobs",
            "schedule": settings.STUCK_JOB_RECOVERY_INTERVAL,
        },
    },
)

celery_app.autodiscover_tasks(["app.tasks", "app.tasks.worker_tasks", "app.tasks.workflows"])
