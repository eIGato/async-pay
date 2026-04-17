import asyncio
import logging
import random
from datetime import UTC, datetime
from uuid import UUID

from async_pay.config import Settings
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
        )
