import uuid
from datetime import datetime

from pydantic import BaseModel, Field

from app.models import JobStatus, JobType


class JobCreate(BaseModel):
    type: JobType
    payload: dict = Field(default_factory=dict)
    priority: int = Field(default=0, ge=0, le=10)
    max_retries: int = Field(default=3, ge=0, le=10)


class JobResponse(BaseModel):
    id: uuid.UUID
    type: JobType
    status: JobStatus
    payload: dict
    result: dict | None = None
    error: str | None = None
    priority: int
    max_retries: int
    retry_count: int
    progress: int = 0
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    celery_task_id: str | None = None
    file_path: str | None = None
    file_name: str | None = None
    file_size: int | None = None

    model_config = {"from_attributes": True}


class JobListResponse(BaseModel):
    jobs: list[JobResponse]
    total: int


class WorkerResponse(BaseModel):
    id: uuid.UUID
    hostname: str
    pid: int
    status: str
    current_task: str | None = None
    registered_at: datetime
    last_heartbeat: datetime

    model_config = {"from_attributes": True}


class WorkerListResponse(BaseModel):
    workers: list[WorkerResponse]
    total: int
    active_count: int = 0
    inactive_count: int = 0


class QueueStats(BaseModel):
    queue_name: str
    depth: int
    workers: int


class StatsResponse(BaseModel):
    total_jobs: int
    jobs_by_status: dict[str, int]
    jobs_by_type: dict[str, int]
    active_workers: int
    queue_depth: int
    avg_processing_time_seconds: float | None = None
    error_rate_percent: float = 0.0
    queues: dict[str, int] = Field(default_factory=dict)


class MessageResponse(BaseModel):
    message: str


class FileUploadResponse(BaseModel):
    file_id: str
    file_name: str
    file_size: int
    message: str


class WorkerRegisterRequest(BaseModel):
    hostname: str
    pid: int


class PipelineStep(BaseModel):
    type: JobType
    payload: dict = Field(default_factory=dict)
    priority: int = Field(default=0, ge=0, le=10)
    max_retries: int = Field(default=3, ge=0, le=10)


class PipelineCreate(BaseModel):
    steps: list[PipelineStep] = Field(..., min_length=1, max_length=10)
    name: str | None = None


class PipelineResponse(BaseModel):
    pipeline_id: uuid.UUID
    job_ids: list[uuid.UUID]
    status: str
    message: str


class DLQItem(BaseModel):
    job_id: str
    task_name: str
    error: str
    payload: dict
    retry_count: int


class DLQResponse(BaseModel):
    items: list[DLQItem]
    total: int


class CircuitBreakerState(BaseModel):
    name: str
    state: str
    failures: int
    failure_threshold: int
    recovery_timeout: int
    last_failure: str | None = None


class CircuitBreakersResponse(BaseModel):
    breakers: list[CircuitBreakerState]


class ProgressResponse(BaseModel):
    job_id: str
    progress: int
    stage: str
    updated_at: float


class HealthCheckResponse(BaseModel):
    status: str
    checks: dict[str, str]
    uptime_seconds: float | None = None
