import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from async_pay.api.routes import payments as payments_router
from async_pay.config import Settings, get_settings
from async_pay.db import create_engine, create_session_factory
from async_pay.logging_config import configure_logging
from async_pay.outbox.publisher import OutboxPublisher

logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine = create_engine(settings)
        session_factory = create_session_factory(engine)
        publisher = OutboxPublisher(session_factory, settings)

        app.state.settings = settings
        app.state.engine = engine
        app.state.session_factory = session_factory
        app.state.publisher = publisher

        await publisher.start()
        logger.info("application started")
        try:
            yield
        finally:
            await publisher.stop()
            await engine.dispose()
            logger.info("application stopped")

    app = FastAPI(title="async-pay", version="0.1.0", lifespan=lifespan)
    app.include_router(payments_router.router)
    return app


app = create_app()
