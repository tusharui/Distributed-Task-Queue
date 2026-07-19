import time
import uuid

import structlog
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from app.api.circuit_breakers import router as circuit_breakers_router
from app.api.dlq import router as dlq_router
from app.api.files import router as files_router
from app.api.health import router as health_router
from app.api.jobs import router as jobs_router
from app.api.pipelines import router as pipelines_router
from app.api.progress import router as progress_router
from app.api.stats import router as stats_router
from app.api.workers import router as workers_router
from app.config import get_settings, setup_logging

settings = get_settings()
setup_logging()
logger = structlog.get_logger(__name__)

app = FastAPI(
    title="Distributed Task Queue",
    description="A distributed job processing platform",
    version="0.2.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class RequestIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(request_id=request_id)

        start = time.perf_counter()
        response: Response = await call_next(request)
        duration = round((time.perf_counter() - start) * 1000, 2)

        response.headers["X-Request-ID"] = request_id
        response.headers["X-Response-Time"] = f"{duration}ms"

        logger.info(
            "request_completed",
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_ms=duration,
        )
        return response


app.add_middleware(RequestIDMiddleware)

app.include_router(health_router, tags=["health"])
app.include_router(jobs_router)
app.include_router(pipelines_router)
app.include_router(files_router)
app.include_router(workers_router)
app.include_router(stats_router)
app.include_router(dlq_router)
app.include_router(circuit_breakers_router)
app.include_router(progress_router)


@app.get("/")
async def root():
    return {"message": "Distributed Task Queue API", "version": "0.2.0"}
