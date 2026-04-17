import logging
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from async_pay.models import OutboxEvent, Payment, PaymentStatus
from async_pay.schemas import CreatePaymentRequest

logger = logging.getLogger(__name__)

PAYMENT_CREATED_EVENT = "payment.created"


async def create_or_get_payment(
    session: AsyncSession,
    idempotency_key: str,
    request: CreatePaymentRequest,
) -> tuple[Payment, bool]:
    existing = await session.scalar(
        select(Payment).where(Payment.idempotency_key == idempotency_key)
    )
    if existing is not None:
        return existing, False

    payment = Payment(
        amount=request.amount,
        currency=request.currency,
        description=request.description,
        meta=request.metadata,
        status=PaymentStatus.PENDING,
        idempotency_key=idempotency_key,
        webhook_url=str(request.webhook_url) if request.webhook_url else None,
    )
    session.add(payment)

    try:
        await session.flush()
    except IntegrityError:
        await session.rollback()
        existing = await session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        if existing is None:
            raise
        return existing, False

    session.add(
        OutboxEvent(
            event_type=PAYMENT_CREATED_EVENT,
            payload={"payment_id": str(payment.id)},
        )
    )
    await session.commit()
    await session.refresh(payment)
    return payment, True


async def get_payment(session: AsyncSession, payment_id: UUID) -> Payment | None:
    return await session.scalar(select(Payment).where(Payment.id == payment_id))
