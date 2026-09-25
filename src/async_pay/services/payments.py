import logging
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from async_pay.models import OutboxEvent, Payment, PaymentStatus
from async_pay.schemas import CreatePaymentRequest

logger = logging.getLogger(__name__)

PAYMENT_CREATED_EVENT = "payment.created"


class IdempotencyConflictError(Exception):
    """An Idempotency-Key was replayed with a different request body."""

    def __init__(self, payment_id: UUID) -> None:
        super().__init__(f"idempotency key already used for payment {payment_id}")
        self.payment_id = payment_id


def request_matches(payment: Payment, request: CreatePaymentRequest) -> bool:
    """Whether ``request`` is a faithful replay of the one that created ``payment``."""
    webhook_url = str(request.webhook_url) if request.webhook_url else None
    return (
        payment.amount == request.amount
        and payment.currency == request.currency
        and payment.description == request.description
        and payment.meta == request.metadata
        and payment.webhook_url == webhook_url
    )


def _replay(payment: Payment, request: CreatePaymentRequest) -> tuple[Payment, bool]:
    if not request_matches(payment, request):
        raise IdempotencyConflictError(payment.id)
    return payment, False


async def create_or_get_payment(
    session: AsyncSession,
    idempotency_key: str,
    request: CreatePaymentRequest,
) -> tuple[Payment, bool]:
    existing = await session.scalar(
        select(Payment).where(Payment.idempotency_key == idempotency_key)
    )
    if existing is not None:
        return _replay(existing, request)

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
        return _replay(existing, request)

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
