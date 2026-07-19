import csv
import json
import random
import uuid
from datetime import UTC, datetime

import structlog
from celery import shared_task
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.config import get_settings
from app.models import Job, JobStatus
from app.utils.circuit_breaker import get_breaker
from app.utils.dead_letter_queue import push_to_dlq
from app.utils.progress import delete_progress, set_progress

logger = structlog.get_logger(__name__)
settings = get_settings()

sync_database_url = settings.DATABASE_URL.replace("+asyncpg", "+psycopg2")
engine = create_engine(sync_database_url, pool_size=5, max_overflow=5)
SyncSession = sessionmaker(bind=engine)


def _get_sync_session() -> Session:
    return SyncSession()


def _update_job_status(
    job_id: uuid.UUID,
    status: JobStatus,
    result: dict | None = None,
    error: str | None = None,
    progress: int | None = None,
    stage: str | None = None,
):
    session = _get_sync_session()
    try:
        job = session.get(Job, job_id)
        if job:
            job.status = status
            job.updated_at = datetime.now(UTC)
            if status == JobStatus.RUNNING:
                job.started_at = datetime.now(UTC)
            elif status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
                job.completed_at = datetime.now(UTC)
            if result is not None:
                job.result = result
            if error is not None:
                job.error = error
            if progress is not None:
                job.progress = progress
            session.commit()
            if progress is not None:
                set_progress(str(job_id), progress, stage=stage or status.value)
            logger.info("job_status_updated", job_id=str(job_id), status=status.value)
    except Exception:
        session.rollback()
        logger.exception("job_status_update_failed", job_id=str(job_id))
    finally:
        session.close()


def _handle_task_failure(job_id: uuid.UUID, task_name: str, exc: Exception, payload: dict, retry_count: int):
    breaker = get_breaker(task_name)
    breaker.record_failure()
    session = _get_sync_session()
    try:
        job = session.get(Job, job_id)
        if job and job.max_retries > 0 and retry_count >= job.max_retries:
            push_to_dlq(
                job_id=str(job_id),
                task_name=task_name,
                error=str(exc),
                payload=payload,
                retry_count=retry_count,
            )
            job.status = JobStatus.FAILED
            job.error = f"Moved to DLQ after {retry_count} retries: {exc}"
            job.completed_at = datetime.now(UTC)
            job.updated_at = datetime.now(UTC)
            session.commit()
            logger.warning("job_moved_to_dlq", job_id=str(job_id), task_name=task_name)
            return
    except Exception:
        session.rollback()
        logger.exception("dlq_check_failed", job_id=str(job_id))
    finally:
        session.close()
    delete_progress(str(job_id))


def _get_backoff(retries: int) -> int:
    base = settings.RETRY_BACKOFF_BASE
    jitter_max = settings.RETRY_BACKOFF_JITTER_MAX
    exponential = base * (2 ** retries)
    jitter = random.randint(0, min(exponential // 2, jitter_max))
    return exponential + jitter


@shared_task(
    name="app.tasks.pdf_summarization",
    bind=True,
    max_retries=5,
    rate_limit="30/m",
    soft_time_limit=300,
    time_limit=600,
)
def pdf_summarization(self, job_id: str, payload: dict):
    job_uuid = uuid.UUID(job_id)
    breaker = get_breaker("pdf_summarization")
    if not breaker.allow_request():
        _update_job_status(job_uuid, JobStatus.FAILED, error="Circuit breaker open")
        return {"error": "circuit_breaker_open"}

    _update_job_status(job_uuid, JobStatus.RUNNING, progress=0, stage="starting")
    try:
        text = payload.get("text", "")
        if not text and payload.get("file_path"):
            try:
                with open(payload["file_path"], errors="ignore") as f:
                    text = f.read()
            except Exception:
                text = ""

        _update_job_status(job_uuid, JobStatus.RUNNING, progress=20, stage="parsing")
        sentences = [s.strip() for s in text.replace("\n", " ").split(".") if s.strip()]
        _update_job_status(job_uuid, JobStatus.RUNNING, progress=50, stage="analyzing")

        word_freq: dict[str, int] = {}
        for word in text.lower().split():
            if len(word) > 3:
                word_freq[word] = word_freq.get(word, 0) + 1
        top_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:10]

        summary = ". ".join(sentences[:3]) + "." if sentences else "No content to summarize."
        word_count = len(text.split())
        _update_job_status(job_uuid, JobStatus.RUNNING, progress=80, stage="summarizing")

        result = {
            "summary": summary,
            "original_word_count": word_count,
            "summary_word_count": len(summary.split()),
            "total_sentences": len(sentences),
            "key_terms": [w for w, _ in top_words],
            "pages_processed": payload.get("pages", 1),
        }
        _update_job_status(job_uuid, JobStatus.COMPLETED, result=result, progress=100, stage="done")
        breaker.record_success()
        return result
    except SoftTimeLimitExceeded as exc:
        _update_job_status(job_uuid, JobStatus.FAILED, error=f"Task timed out: {exc}")
        _handle_task_failure(job_id, "pdf_summarization", exc, payload, self.request.retries)
        raise
    except Exception as exc:
        _update_job_status(job_uuid, JobStatus.RETRYING, error=str(exc))
        _handle_task_failure(job_id, "pdf_summarization", exc, payload, self.request.retries)
        raise self.retry(exc=exc, countdown=_get_backoff(self.request.retries))


@shared_task(
    name="app.tasks.image_resizing",
    bind=True,
    max_retries=5,
    rate_limit="20/m",
    soft_time_limit=300,
    time_limit=600,
)
def image_resizing(self, job_id: str, payload: dict):
    job_uuid = uuid.UUID(job_id)
    breaker = get_breaker("image_resizing")
    if not breaker.allow_request():
        _update_job_status(job_uuid, JobStatus.FAILED, error="Circuit breaker open")
        return {"error": "circuit_breaker_open"}

    _update_job_status(job_uuid, JobStatus.RUNNING, progress=0, stage="starting")
    try:
        _update_job_status(job_uuid, JobStatus.RUNNING, progress=10, stage="loading")

        file_path = payload.get("file_path")
        target_width = payload.get("target_width", 512)
        target_height = payload.get("target_height", 384)
        output_format = payload.get("format", "PNG")

        if file_path:
            from PIL import Image

            _update_job_status(job_uuid, JobStatus.RUNNING, progress=30, stage="opening")
            img = Image.open(file_path)
            original_width, original_height = img.size
            _update_job_status(job_uuid, JobStatus.RUNNING, progress=50, stage="resizing")

            resized = img.resize((target_width, target_height), Image.Resampling.LANCZOS)
            _update_job_status(job_uuid, JobStatus.RUNNING, progress=70, stage="saving")

            output_path = file_path.rsplit(".", 1)[0] + f"_resized.{output_format.lower()}"
            resized.save(output_path, output_format)
            _update_job_status(job_uuid, JobStatus.RUNNING, progress=90, stage="finalizing")

            result = {
                "original_size": {"width": original_width, "height": original_height},
                "new_size": {"width": target_width, "height": target_height},
                "output_path": output_path,
                "format": output_format,
                "mode": img.mode,
            }
        else:
            original_width = payload.get("width", 1024)
            original_height = payload.get("height", 768)
            scale_x = target_width / original_width if original_width else 1
            scale_y = target_height / original_height if original_height else 1
            result = {
                "original_size": {"width": original_width, "height": original_height},
                "new_size": {"width": target_width, "height": target_height},
                "scale_factor": {"x": round(scale_x, 4), "y": round(scale_y, 4)},
                "format": output_format,
            }

        _update_job_status(job_uuid, JobStatus.COMPLETED, result=result, progress=100, stage="done")
        breaker.record_success()
        return result
    except SoftTimeLimitExceeded as exc:
        _update_job_status(job_uuid, JobStatus.FAILED, error=f"Task timed out: {exc}")
        _handle_task_failure(job_id, "image_resizing", exc, payload, self.request.retries)
        raise
    except Exception as exc:
        _update_job_status(job_uuid, JobStatus.RETRYING, error=str(exc))
        _handle_task_failure(job_id, "image_resizing", exc, payload, self.request.retries)
        raise self.retry(exc=exc, countdown=_get_backoff(self.request.retries))


@shared_task(
    name="app.tasks.csv_processing",
    bind=True,
    max_retries=5,
    rate_limit="30/m",
    soft_time_limit=300,
    time_limit=600,
)
def csv_processing(self, job_id: str, payload: dict):
    job_uuid = uuid.UUID(job_id)
    breaker = get_breaker("csv_processing")
    if not breaker.allow_request():
        _update_job_status(job_uuid, JobStatus.FAILED, error="Circuit breaker open")
        return {"error": "circuit_breaker_open"}

    _update_job_status(job_uuid, JobStatus.RUNNING, progress=0, stage="starting")
    try:
        _update_job_status(job_uuid, JobStatus.RUNNING, progress=10, stage="reading")

        file_path = payload.get("file_path")
        if file_path:
            _update_job_status(job_uuid, JobStatus.RUNNING, progress=20, stage="parsing_file")
            with open(file_path, newline="", errors="ignore") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            columns = list(rows[0].keys()) if rows else []
            _update_job_status(job_uuid, JobStatus.RUNNING, progress=50, stage="analyzing")
        else:
            rows = payload.get("rows", [])
            columns = payload.get("columns", [])

        total_rows = len(rows)
        total_columns = len(columns)
        _update_job_status(job_uuid, JobStatus.RUNNING, progress=60, stage="computing")

        numeric_stats: dict[str, dict] = {}
        text_stats: dict[str, dict] = {}
        for col in columns:
            values = [row.get(col, "") for row in rows]
            numeric_vals = []
            for v in values:
                try:
                    numeric_vals.append(float(v))
                except (ValueError, TypeError):
                    pass

            if numeric_vals:
                numeric_stats[col] = {
                    "min": min(numeric_vals),
                    "max": max(numeric_vals),
                    "avg": round(sum(numeric_vals) / len(numeric_vals), 2),
                    "count": len(numeric_vals),
                }
            else:
                unique_vals = list(set(values))
                text_stats[col] = {
                    "unique_count": len(unique_vals),
                    "sample_values": unique_vals[:5],
                }

        _update_job_status(job_uuid, JobStatus.RUNNING, progress=90, stage="aggregating")

        null_counts: dict[str, int] = {}
        for col in columns:
            null_counts[col] = sum(1 for row in rows if not row.get(col))

        result = {
            "total_rows": total_rows,
            "total_columns": total_columns,
            "columns": columns,
            "numeric_stats": numeric_stats,
            "text_stats": text_stats,
            "null_counts": null_counts,
        }
        _update_job_status(job_uuid, JobStatus.COMPLETED, result=result, progress=100, stage="done")
        breaker.record_success()
        return result
    except SoftTimeLimitExceeded as exc:
        _update_job_status(job_uuid, JobStatus.FAILED, error=f"Task timed out: {exc}")
        _handle_task_failure(job_id, "csv_processing", exc, payload, self.request.retries)
        raise
    except Exception as exc:
        _update_job_status(job_uuid, JobStatus.RETRYING, error=str(exc))
        _handle_task_failure(job_id, "csv_processing", exc, payload, self.request.retries)
        raise self.retry(exc=exc, countdown=_get_backoff(self.request.retries))


@shared_task(
    name="app.tasks.ai_text_generation",
    bind=True,
    max_retries=5,
    rate_limit="10/m",
    soft_time_limit=300,
    time_limit=600,
)
def ai_text_generation(self, job_id: str, payload: dict):
    job_uuid = uuid.UUID(job_id)
    breaker = get_breaker("ai_text_generation")
    if not breaker.allow_request():
        _update_job_status(job_uuid, JobStatus.FAILED, error="Circuit breaker open")
        return {"error": "circuit_breaker_open"}

    _update_job_status(job_uuid, JobStatus.RUNNING, progress=0, stage="starting")
    try:
        prompt = payload.get("prompt", "")
        max_tokens = payload.get("max_tokens", 100)
        temperature = payload.get("temperature", 0.7)
        _update_job_status(job_uuid, JobStatus.RUNNING, progress=50, stage="generating")
        generated_text = f"[Simulated AI response for prompt: '{prompt[:100]}...']"
        result = {
            "generated_text": generated_text,
            "prompt": prompt,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "model": payload.get("model", "simulated"),
        }
        _update_job_status(job_uuid, JobStatus.COMPLETED, result=result, progress=100, stage="done")
        breaker.record_success()
        return result
    except SoftTimeLimitExceeded as exc:
        _update_job_status(job_uuid, JobStatus.FAILED, error=f"Task timed out: {exc}")
        _handle_task_failure(job_id, "ai_text_generation", exc, payload, self.request.retries)
        raise
    except Exception as exc:
        _update_job_status(job_uuid, JobStatus.RETRYING, error=str(exc))
        _handle_task_failure(job_id, "ai_text_generation", exc, payload, self.request.retries)
        raise self.retry(exc=exc, countdown=_get_backoff(self.request.retries))


@shared_task(
    name="app.tasks.html_to_text",
    bind=True,
    max_retries=5,
    rate_limit="30/m",
    soft_time_limit=300,
    time_limit=600,
)
def html_to_text(self, job_id: str, payload: dict):
    job_uuid = uuid.UUID(job_id)
    breaker = get_breaker("html_to_text")
    if not breaker.allow_request():
        _update_job_status(job_uuid, JobStatus.FAILED, error="Circuit breaker open")
        return {"error": "circuit_breaker_open"}

    _update_job_status(job_uuid, JobStatus.RUNNING, progress=0, stage="starting")
    try:
        _update_job_status(job_uuid, JobStatus.RUNNING, progress=10, stage="reading")

        file_path = payload.get("file_path")
        html_content = payload.get("html", "")

        if file_path:
            with open(file_path, errors="ignore") as f:
                html_content = f.read()

        _update_job_status(job_uuid, JobStatus.RUNNING, progress=30, stage="parsing")

        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html_content, "html.parser")

        for script in soup(["script", "style"]):
            script.decompose()

        _update_job_status(job_uuid, JobStatus.RUNNING, progress=50, stage="extracting")

        text = soup.get_text(separator="\n", strip=True)
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        clean_text = "\n".join(lines)

        _update_job_status(job_uuid, JobStatus.RUNNING, progress=70, stage="collecting_links")

        links = []
        for a_tag in soup.find_all("a", href=True):
            links.append({"text": a_tag.get_text(strip=True), "href": a_tag["href"]})

        images = []
        for img in soup.find_all("img", src=True):
            images.append({"src": img["src"], "alt": img.get("alt", "")})

        title = ""
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text(strip=True)

        _update_job_status(job_uuid, JobStatus.RUNNING, progress=90, stage="finalizing")

        result = {
            "title": title,
            "text": clean_text,
            "word_count": len(clean_text.split()),
            "line_count": len(lines),
            "links": links[:50],
            "link_count": len(links),
            "images": images[:20],
            "image_count": len(images),
        }
        _update_job_status(job_uuid, JobStatus.COMPLETED, result=result, progress=100, stage="done")
        breaker.record_success()
        return result
    except SoftTimeLimitExceeded as exc:
        _update_job_status(job_uuid, JobStatus.FAILED, error=f"Task timed out: {exc}")
        _handle_task_failure(job_id, "html_to_text", exc, payload, self.request.retries)
        raise
    except Exception as exc:
        _update_job_status(job_uuid, JobStatus.RETRYING, error=str(exc))
        _handle_task_failure(job_id, "html_to_text", exc, payload, self.request.retries)
        raise self.retry(exc=exc, countdown=_get_backoff(self.request.retries))


@shared_task(
    name="app.tasks.data_validation",
    bind=True,
    max_retries=5,
    rate_limit="30/m",
    soft_time_limit=300,
    time_limit=600,
)
def data_validation(self, job_id: str, payload: dict):
    job_uuid = uuid.UUID(job_id)
    breaker = get_breaker("data_validation")
    if not breaker.allow_request():
        _update_job_status(job_uuid, JobStatus.FAILED, error="Circuit breaker open")
        return {"error": "circuit_breaker_open"}

    _update_job_status(job_uuid, JobStatus.RUNNING, progress=0, stage="starting")
    try:
        _update_job_status(job_uuid, JobStatus.RUNNING, progress=10, stage="loading")

        data = payload.get("data", [])
        rules = payload.get("rules", {})
        _update_job_status(job_uuid, JobStatus.RUNNING, progress=30, stage="validating")

        errors: list[dict] = []
        warnings: list[dict] = []
        valid_count = 0

        required_fields = rules.get("required", [])
        field_types = rules.get("types", {})
        ranges = rules.get("ranges", {})

        for idx, item in enumerate(data):
            item_errors: list[str] = []

            for field in required_fields:
                if field not in item or item[field] is None or item[field] == "":
                    item_errors.append(f"Missing required field: {field}")

            for field, expected_type in field_types.items():
                if field in item and item[field] is not None:
                    value = item[field]
                    if expected_type == "int" and not isinstance(value, int):
                        item_errors.append(f"Field '{field}' should be int, got {type(value).__name__}")
                    elif expected_type == "float" and not isinstance(value, (int, float)):
                        item_errors.append(f"Field '{field}' should be float, got {type(value).__name__}")
                    elif expected_type == "str" and not isinstance(value, str):
                        item_errors.append(f"Field '{field}' should be str, got {type(value).__name__}")
                    elif expected_type == "email" and isinstance(value, str):
                        if "@" not in value or "." not in value:
                            item_errors.append(f"Field '{field}' is not a valid email")

            for field, (min_val, max_val) in ranges.items():
                if field in item and isinstance(item[field], (int, float)):
                    if min_val is not None and item[field] < min_val:
                        item_errors.append(f"Field '{field}' is below minimum {min_val}")
                    if max_val is not None and item[field] > max_val:
                        item_errors.append(f"Field '{field}' is above maximum {max_val}")

            if item_errors:
                errors.append({"index": idx, "errors": item_errors, "item": item})
            else:
                valid_count += 1

        _update_job_status(job_uuid, JobStatus.RUNNING, progress=90, stage="aggregating")

        result = {
            "total_records": len(data),
            "valid_records": valid_count,
            "invalid_records": len(errors),
            "error_rate": round(len(errors) / len(data) * 100, 2) if data else 0,
            "errors": errors[:100],
            "warnings": warnings[:100],
        }
        _update_job_status(job_uuid, JobStatus.COMPLETED, result=result, progress=100, stage="done")
        breaker.record_success()
        return result
    except SoftTimeLimitExceeded as exc:
        _update_job_status(job_uuid, JobStatus.FAILED, error=f"Task timed out: {exc}")
        _handle_task_failure(job_id, "data_validation", exc, payload, self.request.retries)
        raise
    except Exception as exc:
        _update_job_status(job_uuid, JobStatus.RETRYING, error=str(exc))
        _handle_task_failure(job_id, "data_validation", exc, payload, self.request.retries)
        raise self.retry(exc=exc, countdown=_get_backoff(self.request.retries))


@shared_task(
    name="app.tasks.file_conversion",
    bind=True,
    max_retries=5,
    rate_limit="20/m",
    soft_time_limit=300,
    time_limit=600,
)
def file_conversion(self, job_id: str, payload: dict):
    job_uuid = uuid.UUID(job_id)
    breaker = get_breaker("file_conversion")
    if not breaker.allow_request():
        _update_job_status(job_uuid, JobStatus.FAILED, error="Circuit breaker open")
        return {"error": "circuit_breaker_open"}

    _update_job_status(job_uuid, JobStatus.RUNNING, progress=0, stage="starting")
    try:
        _update_job_status(job_uuid, JobStatus.RUNNING, progress=10, stage="reading")

        file_path = payload.get("file_path")
        target_format = payload.get("target_format", "json")
        _update_job_status(job_uuid, JobStatus.RUNNING, progress=30, stage="preparing")

        if not file_path:
            raise ValueError("No file_path provided for conversion")

        output_path = file_path.rsplit(".", 1)[0] + f".{target_format}"

        _update_job_status(job_uuid, JobStatus.RUNNING, progress=50, stage="converting")

        source_ext = file_path.rsplit(".", 1)[-1].lower() if "." in file_path else ""

        if source_ext == "csv" and target_format == "json":
            with open(file_path, newline="", errors="ignore") as f:
                reader = csv.DictReader(f)
                data = list(reader)
            with open(output_path, "w") as f:
                json.dump(data, f, indent=2)

        elif source_ext == "json" and target_format == "csv":
            with open(file_path) as f:
                data = json.load(f)
            if data and isinstance(data, list):
                headers = list(data[0].keys())
                with open(output_path, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writeheader()
                    writer.writerows(data)

        elif source_ext in ("txt", "md") and target_format == "json":
            with open(file_path, errors="ignore") as f:
                text = f.read()
            result_data = {
                "content": text,
                "line_count": len(text.splitlines()),
                "word_count": len(text.split()),
            }
            with open(output_path, "w") as f:
                json.dump(result_data, f, indent=2)

        else:
            with open(file_path, errors="ignore") as f:
                content = f.read()
            with open(output_path, "w") as f:
                f.write(content)

        _update_job_status(job_uuid, JobStatus.RUNNING, progress=90, stage="finalizing")

        result = {
            "source_format": source_ext,
            "target_format": target_format,
            "input_path": file_path,
            "output_path": output_path,
        }
        _update_job_status(job_uuid, JobStatus.COMPLETED, result=result, progress=100, stage="done")
        breaker.record_success()
        return result
    except SoftTimeLimitExceeded as exc:
        _update_job_status(job_uuid, JobStatus.FAILED, error=f"Task timed out: {exc}")
        _handle_task_failure(job_id, "file_conversion", exc, payload, self.request.retries)
        raise
    except Exception as exc:
        _update_job_status(job_uuid, JobStatus.RETRYING, error=str(exc))
        _handle_task_failure(job_id, "file_conversion", exc, payload, self.request.retries)
        raise self.retry(exc=exc, countdown=_get_backoff(self.request.retries))


@shared_task(
    name="app.tasks.custom_task",
    bind=True,
    max_retries=5,
    rate_limit="60/m",
    soft_time_limit=300,
    time_limit=600,
)
def custom_task(self, job_id: str, payload: dict):
    job_uuid = uuid.UUID(job_id)
    breaker = get_breaker("custom")
    if not breaker.allow_request():
        _update_job_status(job_uuid, JobStatus.FAILED, error="Circuit breaker open")
        return {"error": "circuit_breaker_open"}

    _update_job_status(job_uuid, JobStatus.RUNNING, progress=0, stage="starting")
    try:
        _update_job_status(job_uuid, JobStatus.RUNNING, progress=50, stage="processing")
        result = {
            "processed_payload": payload,
            "status": "completed",
        }
        _update_job_status(job_uuid, JobStatus.COMPLETED, result=result, progress=100, stage="done")
        breaker.record_success()
        return result
    except SoftTimeLimitExceeded as exc:
        _update_job_status(job_uuid, JobStatus.FAILED, error=f"Task timed out: {exc}")
        _handle_task_failure(job_id, "custom", exc, payload, self.request.retries)
        raise
    except Exception as exc:
        _update_job_status(job_uuid, JobStatus.RETRYING, error=str(exc))
        _handle_task_failure(job_id, "custom", exc, payload, self.request.retries)
        raise self.retry(exc=exc, countdown=_get_backoff(self.request.retries))


TASK_MAP = {
    "pdf_summarization": pdf_summarization,
    "image_resizing": image_resizing,
    "csv_processing": csv_processing,
    "ai_text_generation": ai_text_generation,
    "html_to_text": html_to_text,
    "data_validation": data_validation,
    "file_conversion": file_conversion,
    "custom": custom_task,
}
