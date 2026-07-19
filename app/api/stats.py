from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.schemas import StatsResponse
from app.services import get_stats

router = APIRouter(tags=["stats"])


@router.get("/stats", response_model=StatsResponse)
async def stats_endpoint(
    db: AsyncSession = Depends(get_db),
):
    stats = await get_stats(db)
    return StatsResponse(**stats)
