import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.database import get_db
from app.main import app
from app.models import JobStatus, JobType


async def _mock_db():
    mock = AsyncMock()
    yield mock


@pytest_asyncio.fixture(autouse=True)
def override_db():
    app.dependency_overrides[get_db] = _mock_db
    yield
    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


def _make_job(
    status=JobStatus.QUEUED,
    job_type=JobType.PDF_SUMMARIZATION,
    job_id=None,
):
    job = MagicMock()
    job.id = job_id or uuid.uuid4()
    job.type = job_type
    job.status = status
    job.payload = {"text": "hello"}
    job.result = None
    job.error = None
    job.priority = 0
    job.max_retries = 3
    job.retry_count = 0
    job.progress = 0
    job.created_at = datetime.now(UTC)
    job.updated_at = datetime.now(UTC)
    job.started_at = None
    job.completed_at = None
    job.celery_task_id = "task-123"
    job.file_path = None
    job.file_name = None
    job.file_size = None
    return job


def _make_worker(status="active"):
    worker = MagicMock()
    worker.id = uuid.uuid4()
    worker.hostname = "worker-1"
    worker.pid = 1001
    worker.status = status
    worker.current_task = None
    worker.registered_at = datetime.now(UTC)
    worker.last_heartbeat = datetime.now(UTC)
    return worker


# --- Root & Health Tests ---

@pytest.mark.asyncio
async def test_root(client: AsyncClient):
    response = await client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["message"] == "Distributed Task Queue API"
    assert data["version"] == "0.2.0"


@pytest.mark.asyncio
async def test_health_check(client: AsyncClient):
    response = await client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert "checks" in data
    assert "uptime_seconds" in data


# --- Job CRUD Tests ---

@pytest.mark.asyncio
@patch("app.api.jobs.create_job", new_callable=AsyncMock)
async def test_create_job(mock_create_job, client: AsyncClient):
    mock_create_job.return_value = _make_job()
    response = await client.post(
        "/jobs",
        json={"type": "pdf_summarization", "payload": {"text": "hello"}},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["type"] == "pdf_summarization"
    assert data["status"] == "queued"


@pytest.mark.asyncio
@patch("app.api.jobs.create_job", new_callable=AsyncMock)
async def test_create_html_to_text_job(mock_create_job, client: AsyncClient):
    mock_create_job.return_value = _make_job(job_type=JobType.HTML_TO_TEXT)
    response = await client.post(
        "/jobs",
        json={"type": "html_to_text", "payload": {"html": "<h1>Hello</h1>"}},
    )
    assert response.status_code == 201
    assert response.json()["type"] == "html_to_text"


@pytest.mark.asyncio
@patch("app.api.jobs.create_job", new_callable=AsyncMock)
async def test_create_data_validation_job(mock_create_job, client: AsyncClient):
    mock_create_job.return_value = _make_job(job_type=JobType.DATA_VALIDATION)
    response = await client.post(
        "/jobs",
        json={
            "type": "data_validation",
            "payload": {
                "data": [{"name": "Alice", "age": 30}],
                "rules": {"required": ["name", "age"]},
            },
        },
    )
    assert response.status_code == 201
    assert response.json()["type"] == "data_validation"


@pytest.mark.asyncio
@patch("app.api.jobs.create_job", new_callable=AsyncMock)
async def test_create_file_conversion_job(mock_create_job, client: AsyncClient):
    mock_create_job.return_value = _make_job(job_type=JobType.FILE_CONVERSION)
    response = await client.post(
        "/jobs",
        json={
            "type": "file_conversion",
            "payload": {"file_path": "/tmp/test.csv", "target_format": "json"},
        },
    )
    assert response.status_code == 201
    assert response.json()["type"] == "file_conversion"


@pytest.mark.asyncio
@patch("app.api.jobs.list_jobs", new_callable=AsyncMock)
async def test_list_jobs(mock_list_jobs, client: AsyncClient):
    mock_list_jobs.return_value = ([], 0)
    response = await client.get("/jobs")
    assert response.status_code == 200
    assert response.json() == {"jobs": [], "total": 0}


@pytest.mark.asyncio
@patch("app.api.jobs.list_jobs", new_callable=AsyncMock)
async def test_list_jobs_with_filters(mock_list_jobs, client: AsyncClient):
    mock_list_jobs.return_value = ([], 0)
    response = await client.get("/jobs?status=completed&type=pdf_summarization&limit=10")
    assert response.status_code == 200


@pytest.mark.asyncio
@patch("app.api.jobs.get_job", new_callable=AsyncMock)
async def test_get_job_found(mock_get_job, client: AsyncClient):
    mock_get_job.return_value = _make_job()
    response = await client.get(f"/jobs/{uuid.uuid4()}")
    assert response.status_code == 200


@pytest.mark.asyncio
@patch("app.api.jobs.get_job", new_callable=AsyncMock)
async def test_get_job_not_found(mock_get_job, client: AsyncClient):
    mock_get_job.return_value = None
    response = await client.get(f"/jobs/{uuid.uuid4()}")
    assert response.status_code == 404


@pytest.mark.asyncio
@patch("app.api.jobs.cancel_job", new_callable=AsyncMock)
async def test_cancel_job_success(mock_cancel_job, client: AsyncClient):
    mock_cancel_job.return_value = (_make_job(), "cancelled")
    response = await client.post(f"/jobs/{uuid.uuid4()}/cancel")
    assert response.status_code == 200
    assert "cancelled" in response.json()["message"]


@pytest.mark.asyncio
@patch("app.api.jobs.cancel_job", new_callable=AsyncMock)
async def test_cancel_job_not_found(mock_cancel_job, client: AsyncClient):
    mock_cancel_job.return_value = (None, "not_found")
    response = await client.post(f"/jobs/{uuid.uuid4()}/cancel")
    assert response.status_code == 404


@pytest.mark.asyncio
@patch("app.api.jobs.cancel_job", new_callable=AsyncMock)
async def test_cancel_job_already_completed(mock_cancel_job, client: AsyncClient):
    job = _make_job(status=JobStatus.COMPLETED)
    mock_cancel_job.return_value = (job, "already_terminal")
    response = await client.post(f"/jobs/{job.id}/cancel")
    assert response.status_code == 409


@pytest.mark.asyncio
@patch("app.api.jobs.cancel_job", new_callable=AsyncMock)
async def test_cancel_job_already_failed(mock_cancel_job, client: AsyncClient):
    job = _make_job(status=JobStatus.FAILED)
    mock_cancel_job.return_value = (job, "already_failed")
    response = await client.post(f"/jobs/{job.id}/cancel")
    assert response.status_code == 409


@pytest.mark.asyncio
@patch("app.api.jobs.retry_job", new_callable=AsyncMock)
async def test_retry_job_success(mock_retry_job, client: AsyncClient):
    job = _make_job(status=JobStatus.QUEUED)
    job.retry_count = 1
    mock_retry_job.return_value = (job, "retried")
    response = await client.post(f"/jobs/{job.id}/retry")
    assert response.status_code == 200


@pytest.mark.asyncio
@patch("app.api.jobs.retry_job", new_callable=AsyncMock)
async def test_retry_job_not_failed(mock_retry_job, client: AsyncClient):
    job = _make_job(status=JobStatus.COMPLETED)
    mock_retry_job.return_value = (job, "not_failed")
    response = await client.post(f"/jobs/{job.id}/retry")
    assert response.status_code == 400


@pytest.mark.asyncio
@patch("app.api.jobs.retry_job", new_callable=AsyncMock)
async def test_retry_job_max_retries_exceeded(mock_retry_job, client: AsyncClient):
    job = _make_job(status=JobStatus.FAILED)
    job.retry_count = 3
    job.max_retries = 3
    mock_retry_job.return_value = (job, "max_retries_exceeded")
    response = await client.post(f"/jobs/{job.id}/retry")
    assert response.status_code == 400


@pytest.mark.asyncio
@patch("app.api.jobs.retry_job", new_callable=AsyncMock)
async def test_retry_job_not_found(mock_retry_job, client: AsyncClient):
    mock_retry_job.return_value = (None, "not_found")
    response = await client.post(f"/jobs/{uuid.uuid4()}/retry")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_create_job_validation_error(client: AsyncClient):
    response = await client.post("/jobs", json={"type": "invalid_type"})
    assert response.status_code == 422


# --- Worker Tests ---

@pytest.mark.asyncio
@patch("app.api.workers.get_workers", new_callable=AsyncMock)
async def test_list_workers(mock_get_workers, client: AsyncClient):
    mock_get_workers.return_value = []
    response = await client.get("/workers")
    assert response.status_code == 200
    assert response.json() == {"workers": [], "total": 0, "active_count": 0, "inactive_count": 0}


@pytest.mark.asyncio
@patch("app.api.workers.get_worker", new_callable=AsyncMock)
async def test_get_worker_not_found(mock_get_worker, client: AsyncClient):
    mock_get_worker.return_value = None
    response = await client.get(f"/workers/{uuid.uuid4()}")
    assert response.status_code == 404


@pytest.mark.asyncio
@patch("app.api.workers.get_workers", new_callable=AsyncMock)
async def test_list_workers_with_counts(mock_get_workers, client: AsyncClient):
    mock_get_workers.return_value = [_make_worker("active"), _make_worker("inactive")]
    response = await client.get("/workers")
    assert response.status_code == 200
    data = response.json()
    assert data["active_count"] == 1
    assert data["inactive_count"] == 1


@pytest.mark.asyncio
@patch("app.api.workers.get_worker", new_callable=AsyncMock)
async def test_shutdown_worker_not_found(mock_get_worker, client: AsyncClient):
    mock_get_worker.return_value = None
    response = await client.post(f"/workers/{uuid.uuid4()}/shutdown")
    assert response.status_code == 404


@pytest.mark.asyncio
@patch("app.api.workers.get_worker", new_callable=AsyncMock)
async def test_shutdown_worker_already_inactive(mock_get_worker, client: AsyncClient):
    worker = _make_worker("inactive")
    mock_get_worker.return_value = worker
    response = await client.post(f"/workers/{worker.id}/shutdown")
    assert response.status_code == 409


@pytest.mark.asyncio
@patch("app.tasks.worker_tasks.register_worker.delay")
async def test_register_worker(mock_delay, client: AsyncClient):
    mock_result = MagicMock()
    mock_result.id = "task-reg-1"
    mock_delay.return_value = mock_result
    response = await client.post(
        "/workers/register",
        json={"hostname": "worker-1", "pid": 1234},
    )
    assert response.status_code == 201
    assert "initiated" in response.json()["message"]


# --- Stats Tests ---

@pytest.mark.asyncio
@patch("app.api.stats.get_stats", new_callable=AsyncMock)
async def test_stats(mock_get_stats, client: AsyncClient):
    mock_get_stats.return_value = {
        "total_jobs": 0,
        "jobs_by_status": {},
        "jobs_by_type": {},
        "active_workers": 0,
        "queue_depth": 0,
        "avg_processing_time_seconds": None,
        "error_rate_percent": 0.0,
    }
    response = await client.get("/stats")
    assert response.status_code == 200
    data = response.json()
    assert data["total_jobs"] == 0
    assert "avg_processing_time_seconds" in data
    assert "error_rate_percent" in data


@pytest.mark.asyncio
@patch("app.api.stats.get_stats", new_callable=AsyncMock)
async def test_stats_includes_queues(mock_get_stats, client: AsyncClient):
    mock_get_stats.return_value = {
        "total_jobs": 5,
        "jobs_by_status": {"queued": 3, "completed": 2},
        "jobs_by_type": {"csv_processing": 5},
        "active_workers": 1,
        "queue_depth": 3,
        "avg_processing_time_seconds": 1.5,
        "error_rate_percent": 0.0,
        "queues": {"high": 0, "default": 3, "low": 0},
    }
    response = await client.get("/stats")
    assert response.status_code == 200
    assert response.json()["queues"]["default"] == 3


# --- File Tests ---

@pytest.mark.asyncio
@patch("app.api.files.save_uploaded_file", new_callable=AsyncMock)
async def test_upload_file(mock_save_file, client: AsyncClient):
    mock_save_file.return_value = ("file-123", "/tmp/test.txt", 100)
    mock_task = MagicMock()
    mock_task.delay.return_value = MagicMock(id="task-1")
    with patch.dict("app.tasks.TASK_MAP", {"custom": mock_task}, clear=False):
        response = await client.post(
            "/files/upload",
            files={"file": ("test.txt", b"hello world", "text/plain")},
            params={"job_type": "custom"},
        )
    assert response.status_code == 201
    data = response.json()
    assert data["file_name"] == "test.txt"
    assert data["file_size"] == 100


@pytest.mark.asyncio
async def test_upload_file_no_filename(client: AsyncClient):
    response = await client.post(
        "/files/upload",
        content=b"",
        headers={"Content-Type": "application/octet-stream"},
    )
    assert response.status_code in (400, 422)


@pytest.mark.asyncio
@patch("app.api.files.get_file_path", return_value=None)
async def test_download_file_not_found(mock_get_path, client: AsyncClient):
    response = await client.get("/files/nonexistent/download")
    assert response.status_code == 404


@pytest.mark.asyncio
@patch("app.api.files.get_file_path", return_value="/tmp/test.txt")
@patch("app.api.files.delete_file", return_value=True)
async def test_delete_file_success(mock_delete, mock_get_path, client: AsyncClient):
    response = await client.delete("/files/file-123")
    assert response.status_code == 200
    assert "deleted" in response.json()["message"]


@pytest.mark.asyncio
@patch("app.api.files.get_file_path", return_value=None)
async def test_delete_file_not_found(mock_get_path, client: AsyncClient):
    response = await client.delete("/files/nonexistent")
    assert response.status_code == 404


# --- Pipeline Tests ---

@pytest.mark.asyncio
async def test_create_pipeline(client: AsyncClient):
    mock_task = MagicMock()
    mock_task.apply_async.return_value = MagicMock(id="t1")
    with patch.dict("app.tasks.TASK_MAP", {"pdf_summarization": mock_task, "ai_text_generation": mock_task}, clear=False):
        response = await client.post(
            "/pipelines",
            json={
                "name": "test-pipeline",
                "steps": [
                    {"type": "pdf_summarization", "payload": {"text": "hello"}},
                    {"type": "ai_text_generation", "payload": {"prompt": "summarize"}},
                ],
            },
        )
    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "dispatched"
    assert len(data["job_ids"]) == 2


@pytest.mark.asyncio
async def test_create_pipeline_empty_steps(client: AsyncClient):
    response = await client.post(
        "/pipelines",
        json={"name": "empty", "steps": []},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_create_pipeline_too_many_steps(client: AsyncClient):
    steps = [{"type": "pdf_summarization", "payload": {}} for _ in range(11)]
    response = await client.post(
        "/pipelines",
        json={"name": "too-many", "steps": steps},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
@patch("app.api.pipelines.select")
async def test_get_pipeline_status_not_found(mock_select, client: AsyncClient):
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_select.return_value.where.return_value = mock_select.return_value
    mock_select.return_value.order_by.return_value = mock_select.return_value

    mock_db = AsyncMock()
    mock_db.execute.return_value = mock_result

    async def _override_db():
        yield mock_db

    app.dependency_overrides[get_db] = _override_db
    response = await client.get(f"/pipelines/{uuid.uuid4()}/status")
    assert response.status_code == 404
    app.dependency_overrides[get_db] = _mock_db


# --- DLQ Tests ---

@pytest.mark.asyncio
@patch("app.api.dlq.get_dlq_items", return_value=[])
@patch("app.api.dlq.get_dlq_size", return_value=0)
async def test_list_dlq_empty(mock_size, mock_items, client: AsyncClient):
    response = await client.get("/dlq")
    assert response.status_code == 200
    data = response.json()
    assert data == {"items": [], "total": 0}


@pytest.mark.asyncio
@patch("app.api.dlq.get_dlq_items")
@patch("app.api.dlq.get_dlq_size", return_value=2)
async def test_list_dlq_with_items(mock_size, mock_items, client: AsyncClient):
    mock_items.return_value = [
        {"job_id": "j1", "task_name": "pdf_summarization", "error": "timeout", "payload": {}, "retry_count": 3},
        {"job_id": "j2", "task_name": "ai_text_generation", "error": "circuit open", "payload": {}, "retry_count": 5},
    ]
    response = await client.get("/dlq")
    assert response.status_code == 200
    data = response.json()
    assert len(data["items"]) == 2
    assert data["total"] == 2


@pytest.mark.asyncio
@patch("app.api.dlq.get_dlq_items", return_value=[
    {"job_id": "j1", "task_name": "pdf_summarization", "error": "timeout", "payload": {}, "retry_count": 3},
])
@patch("app.api.dlq.get_dlq_size", return_value=1)
async def test_retry_from_dlq(mock_size, mock_items, client: AsyncClient):
    mock_task = MagicMock()
    mock_task.apply_async.return_value = MagicMock(id="task-retry-1")
    with patch.dict("app.tasks.TASK_MAP", {"pdf_summarization": mock_task}, clear=False):
        response = await client.post("/dlq/j1/retry")
    assert response.status_code == 200
    assert "re-dispatched" in response.json()["message"]


@pytest.mark.asyncio
@patch("app.api.dlq.get_dlq_items", return_value=[])
@patch("app.api.dlq.get_dlq_size", return_value=0)
async def test_retry_from_dlq_not_found(mock_size, mock_items, client: AsyncClient):
    response = await client.post("/dlq/nonexistent/retry")
    assert response.status_code == 404


@pytest.mark.asyncio
@patch("app.api.dlq.clear_dlq")
async def test_clear_dlq(mock_clear, client: AsyncClient):
    response = await client.delete("/dlq")
    assert response.status_code == 200
    assert "cleared" in response.json()["message"]


# --- Circuit Breaker Tests ---

@pytest.mark.asyncio
@patch("app.api.circuit_breakers.get_all_breaker_states")
async def test_list_circuit_breakers(mock_get_states, client: AsyncClient):
    mock_get_states.return_value = [
        {
            "name": "pdf_summarization",
            "state": "closed",
            "failures": 0,
            "failure_threshold": 3,
            "recovery_timeout": 120,
            "last_failure": None,
        },
        {
            "name": "ai_text_generation",
            "state": "open",
            "failures": 5,
            "failure_threshold": 5,
            "recovery_timeout": 60,
            "last_failure": "1234567890",
        },
    ]
    response = await client.get("/circuit-breakers")
    assert response.status_code == 200
    data = response.json()
    assert len(data["breakers"]) == 2
    assert data["breakers"][0]["state"] == "closed"
    assert data["breakers"][1]["state"] == "open"


# --- Progress Tests ---

@pytest.mark.asyncio
@patch("app.api.progress.get_progress")
async def test_get_progress(mock_get_progress, client: AsyncClient):
    mock_get_progress.return_value = {
        "progress": 50,
        "stage": "processing",
        "updated_at": 1234567890.0,
    }
    response = await client.get(f"/progress/{uuid.uuid4()}")
    assert response.status_code == 200
    data = response.json()
    assert data["progress"] == 50
    assert data["stage"] == "processing"


@pytest.mark.asyncio
@patch("app.api.progress.get_progress", return_value=None)
async def test_get_progress_not_found(mock_get_progress, client: AsyncClient):
    response = await client.get(f"/progress/{uuid.uuid4()}")
    assert response.status_code == 404


# --- Priority Queue Tests ---

@pytest.mark.asyncio
@patch("app.api.jobs.create_job", new_callable=AsyncMock)
async def test_create_job_high_priority(mock_create_job, client: AsyncClient):
    mock_create_job.return_value = _make_job()
    response = await client.post(
        "/jobs",
        json={"type": "ai_text_generation", "payload": {"prompt": "test"}, "priority": 9},
    )
    assert response.status_code == 201


@pytest.mark.asyncio
async def test_create_job_priority_out_of_range(client: AsyncClient):
    response = await client.post(
        "/jobs",
        json={"type": "pdf_summarization", "payload": {}, "priority": 15},
    )
    assert response.status_code == 422
