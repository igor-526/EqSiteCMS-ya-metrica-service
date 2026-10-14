from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from settings import settings

async_engine = create_async_engine(
    settings.database_url,
    future=True,
    poolclass=NullPool if settings.debug else None,
)

AsyncSessionLocal = async_sessionmaker(
    async_engine,
    expire_on_commit=False,
    autoflush=False,
)


def make_async_engine(url: str, *, use_null_pool: bool = True):
    return create_async_engine(
        url,
        future=True,
        poolclass=NullPool if use_null_pool else None,
    )


def make_async_session_factory(engine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(
        engine,
        expire_on_commit=False,
        autoflush=False,
    )


async def get_db() -> AsyncIterator[AsyncSession]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def close_database() -> None:
    await async_engine.dispose()
