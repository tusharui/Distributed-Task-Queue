from app.api.health import router as health_router
from app.api.jobs import router as jobs_router
from app.api.stats import router as stats_router
from app.api.workers import router as workers_router

__all__ = [
    "health_router",
    "jobs_router",
    "workers_router",
    "stats_router",
]
