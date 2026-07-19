import enum
import uuid
from datetime import datetime

from sqlalchemy import DateTime, Enum, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class JobStatus(enum.StrEnum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class JobType(enum.StrEnum):
    PDF_SUMMARIZATION = "pdf_summarization"
    IMAGE_RESIZING = "image_resizing"
    CSV_PROCESSING = "csv_processing"
    AI_TEXT_GENERATION = "ai_text_generation"
    HTML_TO_TEXT = "html_to_text"
    DATA_VALIDATION = "data_validation"
    FILE_CONVERSION = "file_conversion"
    CUSTOM = "custom"


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    type: Mapped[JobType] = mapped_column(
        Enum(JobType, values_callable=lambda e: [x.value for x in e]),
        nullable=False,
    )
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus, values_callable=lambda e: [x.value for x in e]),
        nullable=False,
        default=JobStatus.PENDING,
    )
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    result: Mapped[dict | None] = mapped_column(JSON, default=None)
    error: Mapped[str | None] = mapped_column(Text, default=None)
    priority: Mapped[int] = mapped_column(Integer, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, default=3)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    progress: Mapped[int] = mapped_column(Integer, default=0)
    celery_task_id: Mapped[str | None] = mapped_column(
        String(255), default=None
    )
    file_path: Mapped[str | None] = mapped_column(String(500), default=None)
    file_name: Mapped[str | None] = mapped_column(String(255), default=None)
    file_size: Mapped[int | None] = mapped_column(Integer, default=None)


class Worker(Base):
    __tablename__ = "workers"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    hostname: Mapped[str] = mapped_column(String(255), nullable=False)
    pid: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(50), default="active")
    current_task: Mapped[str | None] = mapped_column(
        String(255), default=None
    )
    registered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    last_heartbeat: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
