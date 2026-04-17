import logging

from faststream import FastStream
from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue
from faststream.rabbit.schemas.exchange import ExchangeType

from async_pay.config import Settings, get_settings
from async_pay.consumer.handler import process_payment_event
from async_pay.db import create_engine, create_session_factory
from async_pay.logging_config import configure_logging
from async_pay.rabbit import main_queue_arguments, setup_topology

logger = logging.getLogger(__name__)


def build_app(settings: Settings | None = None) -> FastStream:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    engine = create_engine(settings)
    session_factory = create_session_factory(engine)

    broker = RabbitBroker(settings.rabbitmq_url)

    main_exchange = RabbitExchange(
        settings.payments_exchange,
        type=ExchangeType.DIRECT,
        durable=True,
    )
    main_queue = RabbitQueue(
        settings.payments_queue,
        durable=True,
        routing_key=settings.payments_routing_key,
        arguments=main_queue_arguments(settings),
    )

    dlx = RabbitExchange(
        settings.payments_dlx,
        type=ExchangeType.DIRECT,
        durable=True,
    )
    dlq = RabbitQueue(
        settings.payments_dlq,
        durable=True,
        routing_key=settings.payments_dlq_routing_key,
    )

    @broker.subscriber(main_queue, main_exchange, retry=False)
    async def on_payment_created(body: dict) -> None:
        await process_payment_event(body, session_factory, settings)

    @broker.subscriber(dlq, dlx, retry=False)
    async def on_dead_letter(body: dict) -> None:
        logger.error("dead-lettered payment event: %s", body)

    app = FastStream(broker)

    @app.on_startup
    async def _on_startup() -> None:
        try:
            await setup_topology(settings)
        except Exception:
            logger.exception("failed to set up rabbitmq topology")
            raise

    @app.on_shutdown
    async def _on_shutdown() -> None:
        await engine.dispose()

    return app


app = build_app()
