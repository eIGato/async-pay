from uuid import UUID

import pytest

from async_pay.consumer import handler as handler_module
from async_pay.models import Payment, PaymentStatus


class RecordingMessage:
    """Stands in for a RabbitMessage, recording the broker verdict."""

    def __init__(self, delivery_count: int = 0) -> None:
        self.headers = {"x-delivery-count": delivery_count} if delivery_count else {}
        self.verdict: str | None = None
        self.requeue: bool | None = None

    async def ack(self, multiple: bool = False) -> None:
        self.verdict = "ack"

    async def nack(self, multiple: bool = False, requeue: bool = True) -> None:
        self.verdict = "nack"
        self.requeue = requeue

    async def reject(self, requeue: bool = False) -> None:
        self.verdict = "reject"
        self.requeue = requeue


@pytest.fixture(autouse=True)
def no_sleeping(monkeypatch):
    async def instant(_delay):
        return None

    monkeypatch.setattr(handler_module.asyncio, "sleep", instant)


@pytest.fixture
def instant_settings(settings):
    settings.processing_min_seconds = 0.0
    settings.processing_max_seconds = 0.0
    return settings


async def _make_payment(session_factory, key: str = "handler-key") -> str:
    async with session_factory() as session:
        payment = Payment(amount="10.00", currency="RUB", meta={}, idempotency_key=key)
        session.add(payment)
        await session.commit()
        return str(payment.id)


async def test_success_acks(session_factory, instant_settings):
    payment_id = await _make_payment(session_factory)
    message = RecordingMessage()

    await handler_module.handle_message(
        {"payment_id": payment_id}, message, session_factory, instant_settings
    )

    assert message.verdict == "ack"
    async with session_factory() as session:
        payment = await session.get(Payment, UUID(payment_id))
        assert payment.status is not PaymentStatus.PENDING
        assert payment.processed_at is not None


async def test_unknown_payment_is_acked_not_retried(session_factory, instant_settings):
    message = RecordingMessage()

    await handler_module.handle_message(
        {"payment_id": "00000000-0000-0000-0000-000000000000"},
        message,
        session_factory,
        instant_settings,
    )

    # Nothing to process and nothing a retry could fix.
    assert message.verdict == "ack"


async def test_malformed_payload_is_acked_not_retried(session_factory, instant_settings):
    message = RecordingMessage()

    await handler_module.handle_message({}, message, session_factory, instant_settings)

    assert message.verdict == "ack"


@pytest.mark.parametrize(("delivery_count", "expected_attempt"), [(0, 1), (1, 2)])
async def test_early_failures_are_requeued(
    session_factory, instant_settings, monkeypatch, caplog, delivery_count, expected_attempt
):
    async def boom(*args, **kwargs):
        raise RuntimeError("database on fire")

    monkeypatch.setattr(handler_module, "process_payment_event", boom)
    message = RecordingMessage(delivery_count=delivery_count)

    with caplog.at_level("ERROR"):
        await handler_module.handle_message(
            {"payment_id": "x"}, message, session_factory, instant_settings
        )

    assert message.verdict == "nack"
    assert message.requeue is True
    assert f"attempt {expected_attempt}/3" in caplog.text
    assert "requeueing" in caplog.text


async def test_final_failure_is_rejected_into_the_dlq(
    session_factory, instant_settings, monkeypatch, caplog
):
    async def boom(*args, **kwargs):
        raise RuntimeError("database on fire")

    monkeypatch.setattr(handler_module, "process_payment_event", boom)
    # x-delivery-count == 2 means this is the third and final delivery.
    message = RecordingMessage(delivery_count=2)

    with caplog.at_level("ERROR"):
        await handler_module.handle_message(
            {"payment_id": "x"}, message, session_factory, instant_settings
        )

    # Rejected, not nacked: requeue=False is what routes it to the DLX.
    assert message.verdict == "reject"
    assert message.requeue is False
    assert "attempt 3/3" in caplog.text
    assert "dead-lettering" in caplog.text


async def test_backoff_grows_between_attempts(session_factory, instant_settings, monkeypatch):
    slept: list[float] = []

    async def record_sleep(delay):
        slept.append(delay)

    async def boom(*args, **kwargs):
        raise RuntimeError("nope")

    monkeypatch.setattr(handler_module.asyncio, "sleep", record_sleep)
    monkeypatch.setattr(handler_module, "process_payment_event", boom)

    for delivery_count in (0, 1):
        await handler_module.handle_message(
            {"payment_id": "x"}, RecordingMessage(delivery_count), session_factory, instant_settings
        )

    assert slept == [1.0, 2.0]


async def test_already_finalised_payment_is_skipped(session_factory, instant_settings):
    payment_id = await _make_payment(session_factory, key="already-done")
    message = RecordingMessage()
    await handler_module.handle_message(
        {"payment_id": payment_id}, message, session_factory, instant_settings
    )
    async with session_factory() as session:
        first = await session.get(Payment, UUID(payment_id))
        first_processed_at = first.processed_at
        first_status = first.status

    # A duplicate delivery must not re-process or re-time-stamp the payment.
    await handler_module.handle_message(
        {"payment_id": payment_id}, RecordingMessage(), session_factory, instant_settings
    )

    async with session_factory() as session:
        again = await session.get(Payment, UUID(payment_id))
        assert again.processed_at == first_processed_at
        assert again.status is first_status
