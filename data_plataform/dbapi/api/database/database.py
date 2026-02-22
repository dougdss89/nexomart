from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from typing import AsyncGenerator

from ..config.settings import settings

#cria o engine utilizando a url
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=True, #log para debug
    future=True
    
)

#session factory
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False
)

async def get_session() -> AsyncGenerator[AsyncSession, None]:
    
    async with AsyncSessionLocal() as session:
        try:
            yield session
            
        finally:
            await session.close()