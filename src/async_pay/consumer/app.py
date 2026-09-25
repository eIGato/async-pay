import logging

from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitMessage, RabbitQueue
from faststream.rabbit.schemas.exchange import ExchangeType

from async_pay.config import Settings, get_settings
from async_pay.consumer.handler import handle_message
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

    # MANUAL acking so the last failed attempt can be rejected outright; see
    # handle_message for why x-delivery-limit alone is one delivery too lenient.
    @broker.subscriber(main_queue, main_exchange, ack_policy=AckPolicy.MANUAL)
    async def on_payment_created(body: dict, message: RabbitMessage) -> None:
        await handle_message(body, message, session_factory, settings)

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
