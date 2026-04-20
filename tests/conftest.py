from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from async_pay.api.routes import payments as payments_router
from async_pay.config import Settings
from async_pay.db import SessionFactory
from async_pay.models import Base


@pytest.fixture
def settings() -> Settings:
    return Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        rabbitmq_url="amqp://test@localhost/",
        api_key="test-key",
    )


@pytest_asyncio.fixture
async def session_factory(settings: Settings) -> AsyncIterator[SessionFactory]:
    engine = create_async_engine(
        settings.database_url,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    try:
        yield factory
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def session(session_factory: SessionFactory) -> AsyncIterator[AsyncSession]:
    async with session_factory() as s:
        yield s


@pytest_asyncio.fixture
async def app(settings: Settings, session_factory: SessionFactory) -> FastAPI:
    fast = FastAPI()
    fast.state.settings = settings
    fast.state.session_factory = session_factory
    fast.include_router(payments_router.router)
    return fast


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as c:
        yield c
