import structlog
from fastapi import APIRouter

from app.schemas import CircuitBreakersResponse, CircuitBreakerState
from app.utils.circuit_breaker import get_all_breaker_states

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/circuit-breakers", tags=["circuit-breakers"])


@router.get("", response_model=CircuitBreakersResponse)
async def list_circuit_breakers():
    states = get_all_breaker_states()
    return CircuitBreakersResponse(
        breakers=[CircuitBreakerState(**s) for s in states]
    )
