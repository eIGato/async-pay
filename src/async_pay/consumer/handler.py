import asyncio
import logging
import random
from datetime import UTC, datetime
from uuid import UUID

from faststream.rabbit import RabbitMessage

from async_pay.config import Settings
from async_pay.consumer.retry import attempts_exhausted, backoff_delay, delivery_attempt
from async_pay.consumer.webhook import deliver_webhook
from async_pay.db import SessionFactory
from async_pay.models import Payment, PaymentStatus
from async_pay.schemas import WebhookPayload

logger = logging.getLogger(__name__)


async def process_payment_event(
    payload: dict,
    session_factory: SessionFactory,
    settings: Settings,
) -> None:
    raw_id = payload.get("payment_id")
    if raw_id is None:
        logger.error("received event without payment_id: %s", payload)
        return
    payment_id = UUID(str(raw_id))

    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        if payment is None:
            logger.warning("payment %s not found, skipping", payment_id)
            return
        if payment.status is not PaymentStatus.PENDING:
            logger.info(
                "payment %s already in status=%s, skipping",
                payment_id,
                payment.status.value,
            )
            return

        delay = random.uniform(settings.processing_min_seconds, settings.processing_max_seconds)
        logger.info("processing payment %s (simulated %.2fs)", payment_id, delay)
        await asyncio.sleep(delay)

        succeeded = random.random() < settings.success_probability
        payment.status = PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED
        payment.processed_at = datetime.now(UTC)
        webhook_url = payment.webhook_url
        final_status = payment.status
        processed_at = payment.processed_at
        await session.commit()

    logger.info("payment %s finalised status=%s", payment_id, final_status.value)

    if webhook_url:
        await deliver_webhook(
            webhook_url,
            WebhookPayload(
                payment_id=payment_id,
                status=final_status,
                processed_at=processed_at,
            ),
            timeout=settings.webhook_timeout_seconds,
            max_attempts=settings.webhook_max_attempts,
            retry_base_delay=settings.webhook_retry_base_delay,
        )


async def handle_message(
    body: dict,
    message: RabbitMessage,
    session_factory: SessionFactory,
    settings: Settings,
) -> None:
    """Process one message, deciding its fate explicitly.

    Acks on success. On failure, nacks with requeue and an exponential backoff
    until ``max_delivery_count`` attempts are spent, then rejects so the broker
    dead-letters it. The last attempt is rejected here rather than left to
    ``x-delivery-limit``, which costs one delivery too many: RabbitMQ
    dead-letters a message once its delivery count *exceeds* the limit.
    """
    attempt = delivery_attempt(message)
    try:
        await process_payment_event(body, session_factory, settings)
    except Exception:
        if attempts_exhausted(attempt, settings):
            logger.exception(
                "payment event failed on attempt %d/%d, dead-lettering: %s",
                attempt,
                settings.max_delivery_count,
                body,
            )
            await message.reject()
            return
        delay = backoff_delay(attempt, settings)
        logger.exception(
            "payment event failed on attempt %d/%d, requeueing in %.1fs: %s",
            attempt,
            settings.max_delivery_count,
            delay,
            body,
        )
        await asyncio.sleep(delay)
        await message.nack(requeue=True)
        return
    await message.ack()
