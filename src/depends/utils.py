from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession

from utils.database import AsyncSessionLocal


async def get_session() -> AsyncGenerator[AsyncSession]:
    async with AsyncSessionLocal() as session:
        yield session
