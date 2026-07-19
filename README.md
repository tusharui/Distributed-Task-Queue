# Distributed Task Queue

A distributed job processing platform built with FastAPI, Celery, Redis, and PostgreSQL.

## Architecture

```
Client → FastAPI → PostgreSQL → Celery Producer → Redis Broker → Workers → Process → Update DB → Job Status API
```

## Tech Stack

- **Backend:** Python 3.13, FastAPI, Celery, SQLAlchemy 2.0, Pydantic v2
- **Infrastructure:** Docker, Docker Compose, Nginx
- **Database:** PostgreSQL, Redis
- **Testing:** pytest, httpx

## Quick Start

```bash
docker compose up --build
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | /health | Health check |
| POST | /jobs | Create job |
| GET | /jobs | List jobs |
| GET | /jobs/{id} | Get job |
| DELETE | /jobs/{id} | Cancel job |
| POST | /jobs/{id}/retry | Retry job |
| GET | /workers | List workers |
| GET | /stats | Queue stats |

## Job Types

- PDF summarization
- Image resizing
- CSV processing
- AI text generation
- And more...

## Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run migrations
alembic upgrade head

# Start worker
celery -A app.tasks.celery_app worker -l info
```
