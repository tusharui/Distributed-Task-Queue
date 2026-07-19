# Distributed Task Queue

A production-grade distributed job processing platform built with **FastAPI**, **Celery**, **Redis**, and **PostgreSQL**. Designed to handle AI workloads, file processing, and data pipelines with reliability patterns found in real-world distributed systems.

---

## Table of Contents

- [Architecture](#architecture)
- [System Design](#system-design)
- [Tech Stack](#tech-stack)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Job Types](#job-types)
- [Reliability Patterns](#reliability-patterns)
- [Project Structure](#project-structure)
- [Development](#development)
- [Testing](#testing)

---

## Architecture

```
                    ┌─────────────────────────────────────────────────────┐
                    │                    Nginx (:80)                      │
                    │              Reverse Proxy / Load Balancer          │
                    └──────────────────────┬──────────────────────────────┘
                                           │
                    ┌──────────────────────▼──────────────────────────────┐
                    │                 FastAPI (:8000)                     │
                    │    REST API + WebSocket + Structured Logging        │
                    └───┬──────────┬──────────┬──────────┬───────────────┘
                        │          │          │          │
              ┌─────────▼──┐ ┌────▼────┐ ┌───▼───┐ ┌───▼────┐
              │ PostgreSQL  │ │  Redis  │ │Files  │ │Circuit │
              │  (State)    │ │(Broker) │ │(Disk) │ │Breakers│
              └─────────────┘ └────┬────┘ └───────┘ └────────┘
                                   │
                    ┌──────────────▼──────────────────────────────────────┐
                    │              Celery Worker Pool                     │
                    │   ┌──────────┐ ┌──────────┐ ┌──────────┐          │
                    │   │ Worker 1 │ │ Worker 2 │ │ Worker N │          │
                    │   │(prefork) │ │(prefork) │ │(prefork) │          │
                    │   └──────────┘ └──────────┘ └──────────┘          │
                    │                                                     │
                    │   Queues: high | default | low                     │
                    └────────────────────────────────────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────────────────────┐
                    │              Celery Beat (Scheduler)                │
                    │   • Worker Heartbeat (30s)                          │
                    │   • Stale Worker Cleanup (60s)                      │
                    │   • Stuck Job Recovery (120s)                       │
                    └────────────────────────────────────────────────────┘
```

### Data Flow

1. **Client** sends job request to FastAPI via Nginx
2. **FastAPI** validates request, stores job in PostgreSQL, dispatches to Celery
3. **Celery** routes job to appropriate queue based on priority
4. **Worker** picks up job, processes it, updates progress in Redis + PostgreSQL
5. **Client** polls job status or subscribes to WebSocket for real-time progress
6. On failure: automatic retry with exponential backoff → circuit breaker → dead letter queue

---

## System Design

### Priority-Based Queue Routing

Jobs are routed to queues based on their priority level:

| Priority Range | Queue    | Use Case |
|---------------|----------|----------|
| 7 - 10        | `high`   | AI text generation, urgent tasks |
| 3 - 6         | `default`| PDF summarization, CSV processing, HTML parsing |
| 0 - 2         | `low`    | Background maintenance, non-urgent jobs |

### Circuit Breaker Pattern

Each task type has an independent circuit breaker that prevents cascading failures:

```
CLOSED ──(failures >= threshold)──▶ OPEN ──(timeout elapsed)──▶ HALF_OPEN ──(success)──▶ CLOSED
                                          │
                                          └──(failure)──▶ OPEN
```

- **CLOSED**: Normal operation, requests pass through
- **OPEN**: Task type is failing, requests are rejected immediately
- **HALF_OPEN**: Testing recovery, limited requests allowed

### Dead Letter Queue (DLQ)

When a job exhausts all retries, it's moved to a Redis-backed DLQ:

- **TTL**: 7 days (configurable)
- **Max size**: 1,000 items (FIFO eviction)
- **Operations**: List, retry, clear

### Exponential Backoff with Jitter

Failed tasks retry with exponential backoff plus random jitter to prevent thundering herds:

```
delay = base * 2^retries + random(0, min(base * 2^retries / 2, jitter_max))
```

Default: base=10s, jitter_max=30s → delays of ~10s, ~20s, ~40s, ~80s...

### Real-Time Progress Tracking

Task progress is published to Redis Pub/Sub channels, enabling:

- Fast polling via `GET /progress/{job_id}`
- WebSocket streaming (planned)
- Automatic expiration (configurable TTL)

### Stuck Job Recovery

A Celery Beat task runs every 120s to detect and fail jobs stuck in `RUNNING` state for more than 600s (configurable), preventing resource leaks.

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **API Framework** | FastAPI 0.115 | Async REST API with automatic OpenAPI docs |
| **Task Queue** | Celery 5.4 | Distributed task execution with Redis broker |
| **Database** | PostgreSQL 16 | Job/worker state persistence |
| **Cache/Broker** | Redis 7 | Message broker, progress tracking, circuit breaker state |
| **ORM** | SQLAlchemy 2.0 | Async database access (asyncpg) + sync access (psycopg2) for Celery |
| **Validation** | Pydantic v2 | Request/response schema validation |
| **Logging** | structlog | Structured JSON logging |
| **Reverse Proxy** | Nginx | Load balancing, rate limiting, static file serving |
| **Containerization** | Docker Compose | Multi-service orchestration |
| **Testing** | pytest + httpx | Async API testing with mock dependencies |

---

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.12+ (for local development)

### Docker (Recommended)

```bash
# Clone the repository
git clone https://github.com/yourusername/distributed-task-queue.git
cd distributed-task-queue

# Create environment file
cp .env.example .env
# Edit .env with your values (especially POSTGRES_PASSWORD)

# Start all services
docker compose up --build

# Create database tables (in another terminal)
docker compose exec api python -c "
from sqlalchemy import create_engine
from app.database import Base
from app.config import get_settings
s = get_settings()
e = create_engine(s.DATABASE_URL.replace('+asyncpg','+psycopg2'))
Base.metadata.create_all(e)
print('Tables created!')
"
```

### Verify

```bash
# Health check
curl http://localhost/health

# Create a job
curl -X POST http://localhost/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "html_to_text", "payload": {"html": "<h1>Hello World</h1><p>This is a test.</p>"}}'

# List jobs
curl http://localhost/jobs

# Check stats
curl http://localhost/stats
```

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Copy and configure environment
cp .env.example .env

# Start PostgreSQL and Redis (via Docker or local)
docker compose up -d postgres redis

# Create tables
python -c "
from sqlalchemy import create_engine
from app.database import Base
from app.config import get_settings
s = get_settings()
e = create_engine(s.DATABASE_URL.replace('+asyncpg','+psycopg2'))
Base.metadata.create_all(e)
"

# Start API server
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Start Celery worker (in another terminal)
celery -A app.tasks.celery_app worker -l info -Q high,default,low

# Start Celery beat (in another terminal)
celery -A app.tasks.celery_app beat -l info
```

---

## Configuration

All configuration is managed via environment variables. See `.env.example` for the complete list.

### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | `postgresql+asyncpg://user:pass@host:5432/db` |
| `CELERY_BROKER_URL` | Redis broker URL | `redis://localhost:6379/0` |
| `CELERY_RESULT_BACKEND` | Redis result backend URL | `redis://localhost:6379/0` |
| `POSTGRES_PASSWORD` | PostgreSQL password | `changeme_in_production` |

### Optional Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `API_PORT` | `8000` | API server port |
| `LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |
| `MAX_FILE_SIZE_MB` | `50` | Maximum upload file size in MB |
| `CELERY_SOFT_TIME_LIMIT` | `300` | Task soft timeout in seconds |
| `CELERY_TIME_LIMIT` | `600` | Task hard timeout in seconds |
| `DB_POOL_SIZE` | `20` | Database connection pool size |
| `HEARTBEAT_INTERVAL` | `30` | Worker heartbeat interval in seconds |
| `STUCK_JOB_TIMEOUT` | `600` | Time before a running job is marked stuck |
| `CB_DEFAULT_FAILURE_THRESHOLD` | `5` | Circuit breaker failure threshold |
| `DLQ_MAX_SIZE` | `1000` | Maximum dead letter queue size |
| `RETRY_BACKOFF_BASE` | `10` | Base delay for exponential backoff |
| `DEFAULT_PAGE_SIZE` | `20` | Default pagination page size |

---

## API Reference

### Interactive Documentation

Visit `http://localhost/docs` for Swagger UI or `http://localhost/redoc` for ReDoc.

### Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | Root endpoint with version info |
| `GET` | `/health` | Health check (PostgreSQL, Redis, Celery, circuit breakers, DLQ) |
| **Jobs** | | |
| `POST` | `/jobs` | Create and dispatch a new job |
| `GET` | `/jobs` | List jobs with optional filters (`status`, `type`, `offset`, `limit`) |
| `GET` | `/jobs/{job_id}` | Get job details by ID |
| `POST` | `/jobs/{job_id}/cancel` | Cancel a running/queued job |
| `POST` | `/jobs/{job_id}/retry` | Retry a failed job |
| `DELETE` | `/jobs/{job_id}` | Delete/cancel a job |
| **Pipelines** | | |
| `POST` | `/pipelines` | Create a multi-step job pipeline |
| `GET` | `/pipelines/{pipeline_id}/status` | Get pipeline execution status |
| **Files** | | |
| `POST` | `/files/upload` | Upload a file and optionally create a job |
| `GET` | `/files/{file_id}/download` | Download a file |
| `DELETE` | `/files/{file_id}` | Delete a file |
| **Workers** | | |
| `GET` | `/workers` | List all workers with active/inactive counts |
| `GET` | `/workers/{worker_id}` | Get worker details |
| `POST` | `/workers/register` | Register a new worker |
| `POST` | `/workers/{worker_id}/shutdown` | Send shutdown signal to worker |
| **Observability** | | |
| `GET` | `/stats` | Queue statistics and job metrics |
| `GET` | `/progress/{job_id}` | Real-time progress from Redis |
| `GET` | `/circuit-breakers` | Circuit breaker states for all task types |
| **Dead Letter Queue** | | |
| `GET` | `/dlq` | List failed jobs in DLQ |
| `POST` | `/dlq/{job_id}/retry` | Re-dispatch a job from DLQ |
| `DELETE` | `/dlq` | Clear the dead letter queue |

### Example: Create Job

```bash
curl -X POST http://localhost/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "csv_processing",
    "payload": {
      "rows": [
        {"name": "Alice", "age": "30", "salary": "75000"},
        {"name": "Bob", "age": "25", "salary": "65000"}
      ],
      "columns": ["name", "age", "salary"]
    },
    "priority": 5
  }'
```

### Example: Create Pipeline

```bash
curl -X POST http://localhost/pipelines \
  -H "Content-Type: application/json" \
  -d '{
    "name": "data-processing-pipeline",
    "steps": [
      {"type": "csv_processing", "payload": {"rows": [{"col": "val"}], "columns": ["col"]}},
      {"type": "data_validation", "payload": {"data": [{"name": "test"}], "rules": {"required": ["name"]}}}
    ]
  }'
```

---

## Job Types

| Type | Description | Rate Limit | Queue |
|------|-------------|------------|-------|
| `pdf_summarization` | Extract key terms and summary from text/PDF | 30/min | default |
| `image_resizing` | Resize images using Pillow with LANCZOS resampling | 20/min | default |
| `csv_processing` | Analyze CSV data with numeric/text statistics | 30/min | default |
| `ai_text_generation` | Simulated AI text generation with configurable parameters | 10/min | high |
| `html_to_text` | Parse HTML, extract text, links, and images using BeautifulSoup | 30/min | default |
| `data_validation` | Validate data against configurable rules (required, types, ranges) | 30/min | default |
| `file_conversion` | Convert between formats (CSV↔JSON, TXT→JSON) | 20/min | default |
| `custom` | Generic task for custom payloads | 60/min | default |

---

## Reliability Patterns

### 1. Automatic Retries with Exponential Backoff

Failed tasks are retried up to `max_retries` times with exponential backoff and jitter:

```
Attempt 1: ~10s delay
Attempt 2: ~20s delay
Attempt 3: ~40s delay
Attempt 4: ~80s delay
Attempt 5: Moved to DLQ
```

### 2. Circuit Breaker

Prevents cascading failures by tracking consecutive failures per task type:

- **Threshold**: 5 consecutive failures (configurable per task)
- **Recovery**: 60 seconds (configurable)
- **States**: Closed → Open → Half-Open → Closed

### 3. Dead Letter Queue

Permanently failed jobs are moved to a Redis-backed DLQ instead of being lost:

- Automatic eviction after 7 days
- Manual retry via API
- Size limit with FIFO eviction

### 4. Worker Health Monitoring

- **Heartbeat**: Workers report status every 30 seconds
- **Stale Detection**: Workers silent for 90+ seconds marked inactive
- **Stuck Job Recovery**: Jobs running 10+ minutes auto-failed

### 5. Priority Queue Routing

Three-tier priority system ensures critical jobs (AI generation) are processed before bulk jobs (CSV processing).

### 6. Graceful Shutdown

Workers handle SIGTERM gracefully, completing current tasks before shutting down.

---

## Project Structure

```
distributed-task-queue/
├── app/
│   ├── __init__.py
│   ├── main.py                  # FastAPI app, middleware, routers
│   ├── config.py                # Pydantic Settings (env-based config)
│   ├── database.py              # Async SQLAlchemy engine + session
│   ├── models/
│   │   └── __init__.py          # SQLAlchemy models (Job, Worker)
│   ├── schemas/
│   │   └── __init__.py          # Pydantic request/response schemas
│   ├── api/
│   │   ├── health.py            # GET /health
│   │   ├── jobs.py              # CRUD + cancel + retry
│   │   ├── pipelines.py         # Multi-step pipeline creation
│   │   ├── files.py             # File upload/download/delete
│   │   ├── workers.py           # Worker management
│   │   ├── stats.py             # Queue statistics
│   │   ├── dlq.py               # Dead letter queue management
│   │   ├── circuit_breakers.py  # Circuit breaker states
│   │   └── progress.py          # Real-time progress tracking
│   ├── services/
│   │   └── __init__.py          # Business logic + queue routing
│   ├── tasks/
│   │   ├── __init__.py          # 8 Celery task implementations
│   │   ├── celery_app.py        # Celery config + beat schedule
│   │   ├── worker_tasks.py      # Heartbeat, cleanup, stuck recovery
│   │   └── workflows.py         # Chain/chord/group workflow support
│   └── utils/
│       ├── circuit_breaker.py   # Redis-backed circuit breaker
│       ├── dead_letter_queue.py # Redis-backed DLQ
│       └── progress.py          # Redis-backed progress tracking
├── tests/
│   └── test_api.py              # 46 async API tests
├── docker-compose.yml           # 6-service orchestration
├── Dockerfile                   # Multi-stage Python 3.12 image
├── nginx.conf                   # Reverse proxy config
├── requirements.txt             # Python dependencies
├── pyproject.toml               # Project metadata + tool config
├── .env.example                 # Environment template
└── .gitignore                   # Git ignore rules
```

---

## Development

### Running Tests

```bash
# Run all tests
python -m pytest tests/ -v

# Run with coverage
python -m pytest tests/ -v --cov=app --cov-report=html
```

### Linting

```bash
# Check for lint issues
python -m ruff check app/ tests/

# Auto-fix
python -m ruff check app/ tests/ --fix
```

### Adding a New Job Type

1. Add enum value to `JobType` in `app/models/__init__.py`
2. Implement task function in `app/tasks/__init__.py`
3. Add task to `TASK_MAP` in `app/tasks/__init__.py`
4. Add route in `app/tasks/celery_app.py`
5. Add circuit breaker config in `app/utils/circuit_breaker.py`
6. Write tests in `tests/test_api.py`

---

## Environment Variables Reference

| Category | Variable | Default | Description |
|----------|----------|---------|-------------|
| **Database** | `DATABASE_URL` | `postgresql+asyncpg://...` | Full connection string |
| | `POSTGRES_USER` | `postgres` | Database user |
| | `POSTGRES_PASSWORD` | *(required)* | Database password |
| | `POSTGRES_DB` | `taskqueue` | Database name |
| **Redis** | `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| **Celery** | `CELERY_BROKER_URL` | `redis://localhost:6379/0` | Broker URL |
| | `CELERY_RESULT_BACKEND` | `redis://localhost:6379/0` | Result backend URL |
| | `CELERY_SOFT_TIME_LIMIT` | `300` | Soft timeout (seconds) |
| | `CELERY_TIME_LIMIT` | `600` | Hard timeout (seconds) |
| | `CELERY_RESULT_EXPIRES` | `3600` | Result TTL (seconds) |
| **API** | `API_HOST` | `0.0.0.0` | Bind host |
| | `API_PORT` | `8000` | Bind port |
| | `APP_VERSION` | `0.2.0` | Application version |
| | `CORS_ORIGINS` | `["*"]` | Allowed CORS origins |
| **Upload** | `UPLOAD_DIR` | `uploads` | File storage directory |
| | `MAX_FILE_SIZE_MB` | `50` | Max upload size |
| **Pool** | `DB_POOL_SIZE` | `20` | DB connection pool size |
| | `DB_MAX_OVERFLOW` | `10` | DB pool overflow limit |
| **Worker** | `HEARTBEAT_INTERVAL` | `30` | Heartbeat interval (seconds) |
| | `STALE_WORKER_TIMEOUT` | `90` | Worker stale threshold |
| | `STUCK_JOB_TIMEOUT` | `600` | Stuck job threshold |
| **Circuit Breaker** | `CB_DEFAULT_FAILURE_THRESHOLD` | `5` | Failures before open |
| | `CB_DEFAULT_RECOVERY_TIMEOUT` | `60` | Recovery cooldown |
| **DLQ** | `DLQ_MAX_SIZE` | `1000` | Max queue size |
| | `DLQ_TTL_SECONDS` | `604800` | Item TTL (7 days) |
| **Retry** | `RETRY_BACKOFF_BASE` | `10` | Backoff base delay |
| | `RETRY_BACKOFF_JITTER_MAX` | `30` | Max jitter |
| **Pagination** | `DEFAULT_PAGE_SIZE` | `20` | Default page size |
| | `MAX_PAGE_SIZE` | `100` | Max allowed page size |
| **Logging** | `LOG_LEVEL` | `INFO` | Log level |

---

## License

MIT
