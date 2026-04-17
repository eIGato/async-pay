from decimal import Decimal
from uuid import uuid4

from sqlalchemy import select

from async_pay.models import Currency, OutboxEvent, PaymentStatus
from async_pay.schemas import CreatePaymentRequest
from async_pay.services import payments as service


async def test_create_payment_persists_outbox(session):
    req = CreatePaymentRequest(
        amount=Decimal("10.00"), currency=Currency.RUB, metadata={"order": "42"}
    )
    payment, created = await service.create_or_get_payment(session, "idem-1", req)

    assert created is True
    assert payment.status is PaymentStatus.PENDING
    assert payment.meta == {"order": "42"}

    events = (await session.scalars(select(OutboxEvent))).all()
    assert len(events) == 1
    assert events[0].event_type == service.PAYMENT_CREATED_EVENT
    assert events[0].payload == {"payment_id": str(payment.id)}
    assert events[0].published_at is None


async def test_idempotent_replay_returns_same_payment(session):
    req = CreatePaymentRequest(amount=Decimal("10.00"), currency=Currency.RUB)
    first, created_first = await service.create_or_get_payment(session, "idem-2", req)
    second, created_second = await service.create_or_get_payment(session, "idem-2", req)

    assert created_first is True
    assert created_second is False
    assert first.id == second.id

    events = (await session.scalars(select(OutboxEvent))).all()
    assert len(events) == 1


async def test_get_payment_returns_none_for_missing(session):
    result = await service.get_payment(session, uuid4())
    assert result is None


async def test_get_payment_returns_existing(session):
    req = CreatePaymentRequest(amount=Decimal("5.00"), currency=Currency.EUR)
    created, _ = await service.create_or_get_payment(session, "idem-3", req)

    fetched = await service.get_payment(session, created.id)
    assert fetched is not None
    assert fetched.id == created.id
    assert fetched.currency is Currency.EUR
