import logging

import aio_pika

from async_pay.config import Settings

logger = logging.getLogger(__name__)


def main_queue_arguments(settings: Settings) -> dict[str, object]:
    return {
        "x-queue-type": "quorum",
        "x-delivery-limit": settings.max_delivery_count,
        "x-dead-letter-exchange": settings.payments_dlx,
        "x-dead-letter-routing-key": settings.payments_dlq_routing_key,
    }


async def setup_topology(settings: Settings) -> None:
    """Declare exchanges, queues and bindings. Idempotent."""
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    try:
        channel = await connection.channel()

        dlx = await channel.declare_exchange(
            settings.payments_dlx,
            aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        dlq = await channel.declare_queue(settings.payments_dlq, durable=True)
        await dlq.bind(dlx, routing_key=settings.payments_dlq_routing_key)

        main_exchange = await channel.declare_exchange(
            settings.payments_exchange,
            aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        main_queue = await channel.declare_queue(
            settings.payments_queue,
            durable=True,
            arguments=main_queue_arguments(settings),
        )
        await main_queue.bind(main_exchange, routing_key=settings.payments_routing_key)
        logger.info("rabbitmq topology declared")
    finally:
        await connection.close()
