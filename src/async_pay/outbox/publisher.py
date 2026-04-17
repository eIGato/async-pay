import asyncio
import json
import logging
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue
from faststream.rabbit.schemas.exchange import ExchangeType
from sqlalchemy import select, update

from async_pay.config import Settings
from async_pay.db import SessionFactory
from async_pay.models import OutboxEvent

logger = logging.getLogger(__name__)


class OutboxPublisher:
    """Periodically drains the outbox table into RabbitMQ.

    Runs as a background asyncio task. Idempotency relative to the broker is
    provided by the publish-then-mark flow: any record without ``published_at``
    is eligible for retry on the next tick.
    """

    def __init__(self, session_factory: SessionFactory, settings: Settings) -> None:
        self._session_factory = session_factory
        self._settings = settings
        self._broker: RabbitBroker | None = None
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        self._broker = RabbitBroker(self._settings.rabbitmq_url)
        try:
            await self._broker.connect()
            await self._declare_topology(self._broker)
        except Exception:  # noqa: BLE001
            logger.exception("outbox publisher failed to connect to rabbitmq, will retry")
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="outbox-publisher")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None
        if self._broker is not None:
            try:
                await self._broker.close()
            except Exception:  # noqa: BLE001
                logger.exception("error while closing rabbitmq broker")
            self._broker = None

    async def _declare_topology(self, broker: RabbitBroker) -> None:
        settings = self._settings
        dlx = RabbitExchange(settings.payments_dlx, type=ExchangeType.DIRECT, durable=True)
        dlq = RabbitQueue(settings.payments_dlq, durable=True, routing_key=settings.payments_dlq_routing_key)
        main_exchange = RabbitExchange(
            settings.payments_exchange, type=ExchangeType.DIRECT, durable=True
        )
        main_queue = RabbitQueue(
            settings.payments_queue,
            durable=True,
            routing_key=settings.payments_routing_key,
            arguments={
                "x-dead-letter-exchange": settings.payments_dlx,
                "x-dead-letter-routing-key": settings.payments_dlq_routing_key,
            },
        )
        await broker.declare_exchange(dlx)
        await broker.declare_queue(dlq)
        await broker.declare_exchange(main_exchange)
        await broker.declare_queue(main_queue)

    async def _run(self) -> None:
        interval = self._settings.outbox_poll_interval
        while not self._stop_event.is_set():
            try:
                await self._drain_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("outbox publisher tick failed")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue

    async def _drain_once(self) -> int:
        broker = self._broker
        if broker is None:
            return 0

        async with self._session_factory() as session:
            rows = (
                await session.scalars(
                    select(OutboxEvent)
                    .where(OutboxEvent.published_at.is_(None))
                    .order_by(OutboxEvent.created_at)
                    .limit(self._settings.outbox_batch_size)
                )
            ).all()

            published_ids: list = []
            for row in rows:
                try:
                    await broker.publish(
                        json.dumps(row.payload).encode(),
                        exchange=self._settings.payments_exchange,
                        routing_key=self._settings.payments_routing_key,
                        content_type="application/json",
                        message_id=str(row.id),
                    )
                    published_ids.append(row.id)
                except Exception:  # noqa: BLE001
                    logger.exception("failed to publish outbox event id=%s", row.id)

            if published_ids:
                await session.execute(
                    update(OutboxEvent)
                    .where(OutboxEvent.id.in_(published_ids))
                    .values(published_at=datetime.now(UTC))
                )
                await session.commit()
                logger.info("published %d outbox events", len(published_ids))

            return len(published_ids)
