import threading

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.config import get_settings

settings = get_settings()

_engine = None
_session_factory = None
_lock = threading.Lock()


def _get_engine():
    global _engine, _session_factory
    if _engine is None:
        with _lock:
            if _engine is None:
                _engine = create_async_engine(
                    settings.DATABASE_URL,
                    echo=False,
                    pool_size=20,
                    max_overflow=10,
                )
                _session_factory = async_sessionmaker(
                    _engine, class_=AsyncSession, expire_on_commit=False
                )
    return _engine


def _get_session_factory():
    _get_engine()
    return _session_factory


class Base(DeclarativeBase):
    pass


async def get_db():
    factory = _get_session_factory()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
