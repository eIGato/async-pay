import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import select

from async_pay.config import Settings
from async_pay.db import SessionFactory
from async_pay.models import OutboxEvent
from async_pay.outbox.publisher import OutboxPublisher


class FakeBroker:
    """Records publishes; optionally fails for chosen payment ids."""

    def __init__(self, fail_for: set[str] | None = None) -> None:
        self.published: list[dict] = []
        self.fail_for = fail_for or set()
        self.closed = False
        self.did_publish = asyncio.Event()

    async def publish(self, body: bytes, **kwargs) -> None:
        payload = json.loads(body)
        if payload.get("payment_id") in self.fail_for:
            raise RuntimeError("broker unavailable")
        self.published.append({"payload": payload, **kwargs})
        self.did_publish.set()

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True


async def add_events(factory: SessionFactory, count: int, *, base_offset: int = 0) -> list[str]:
    """Insert `count` unpublished events with strictly increasing created_at."""
    created = datetime(2026, 1, 1, tzinfo=UTC)
    ids = []
    async with factory() as session:
        for i in range(count):
            payment_id = f"00000000-0000-0000-0000-{base_offset + i:012d}"
            session.add(
                OutboxEvent(
                    event_type="payment.created",
                    payload={"payment_id": payment_id},
                    created_at=created + timedelta(seconds=i),
                )
            )
            ids.append(payment_id)
        await session.commit()
    return ids


async def unpublished_payload_ids(factory: SessionFactory) -> set[str]:
    async with factory() as session:
        rows = (
            await session.scalars(select(OutboxEvent).where(OutboxEvent.published_at.is_(None)))
        ).all()
        return {row.payload["payment_id"] for row in rows}


@pytest_asyncio.fixture
async def publisher(session_factory: SessionFactory, settings: Settings) -> OutboxPublisher:
    return OutboxPublisher(session_factory, settings)


async def test_drain_publishes_unpublished_events(publisher, session_factory, settings):
    ids = await add_events(session_factory, 3)
    broker = FakeBroker()
    publisher._broker = broker

    assert await publisher._drain_once() == 3

    assert [p["payload"]["payment_id"] for p in broker.published] == ids
    sent = broker.published[0]
    assert sent["exchange"] == settings.payments_exchange
    assert sent["routing_key"] == settings.payments_routing_key
    assert sent["content_type"] == "application/json"
    assert sent["message_id"]
    assert await unpublished_payload_ids(session_factory) == set()


async def test_drain_marks_rows_published_exactly_once(publisher, session_factory):
    await add_events(session_factory, 2)
    broker = FakeBroker()
    publisher._broker = broker

    assert await publisher._drain_once() == 2
    # A second tick must find nothing left to do — no duplicate publishes.
    assert await publisher._drain_once() == 0
    assert len(broker.published) == 2


async def test_drain_keeps_failed_rows_for_the_next_tick(publisher, session_factory):
    ids = await add_events(session_factory, 3)
    broker = FakeBroker(fail_for={ids[1]})
    publisher._broker = broker

    assert await publisher._drain_once() == 2

    # The row that could not be published stays unpublished and is retried.
    assert await unpublished_payload_ids(session_factory) == {ids[1]}
    publisher._broker = FakeBroker()
    assert await publisher._drain_once() == 1
    assert await unpublished_payload_ids(session_factory) == set()


async def test_drain_publishes_in_created_at_order(publisher, session_factory):
    ids = await add_events(session_factory, 5)
    broker = FakeBroker()
    publisher._broker = broker

    await publisher._drain_once()

    assert [p["payload"]["payment_id"] for p in broker.published] == ids


async def test_drain_honours_batch_size(publisher, session_factory, settings):
    ids = await add_events(session_factory, 5)
    settings.outbox_batch_size = 2
    broker = FakeBroker()
    publisher._broker = broker

    assert await publisher._drain_once() == 2
    assert [p["payload"]["payment_id"] for p in broker.published] == ids[:2]
    assert await unpublished_payload_ids(session_factory) == set(ids[2:])


async def test_drain_is_a_noop_without_a_broker(publisher, session_factory):
    await add_events(session_factory, 2)
    publisher._broker = None

    assert await publisher._drain_once() == 0
    assert len(await unpublished_payload_ids(session_factory)) == 2


async def test_drain_ignores_already_published_rows(publisher, session_factory):
    await add_events(session_factory, 1)
    async with session_factory() as session:
        row = await session.scalar(select(OutboxEvent))
        row.published_at = datetime.now(UTC)
        await session.commit()
    broker = FakeBroker()
    publisher._broker = broker

    assert await publisher._drain_once() == 0
    assert broker.published == []


async def test_background_loop_drains_and_stops(session_factory, settings, monkeypatch):
    broker = FakeBroker()
    monkeypatch.setattr("async_pay.outbox.publisher.RabbitBroker", lambda url: broker)

    async def fake_topology(_settings):
        return None

    monkeypatch.setattr("async_pay.outbox.publisher.setup_topology", fake_topology)

    settings.outbox_poll_interval = 0.01
    publisher = OutboxPublisher(session_factory, settings)
    await add_events(session_factory, 1)

    await publisher.start()
    try:
        async with asyncio.timeout(2):
            await broker.did_publish.wait()
    finally:
        await publisher.stop()

    assert len(broker.published) == 1
    assert broker.closed
    assert publisher._task is None


async def test_start_survives_an_unreachable_broker(session_factory, settings, monkeypatch):
    class DeadBroker(FakeBroker):
        async def connect(self):
            raise RuntimeError("connection refused")

    async def failing_topology(_settings):
        raise RuntimeError("rabbit is down")

    monkeypatch.setattr("async_pay.outbox.publisher.RabbitBroker", lambda url: DeadBroker())
    monkeypatch.setattr("async_pay.outbox.publisher.setup_topology", failing_topology)

    publisher = OutboxPublisher(session_factory, settings)
    await publisher.start()
    try:
        assert publisher._task is not None
    finally:
        await publisher.stop()


async def test_loop_keeps_running_after_a_failing_tick(publisher, monkeypatch):
    calls = []
    recovered = asyncio.Event()

    async def flaky_drain():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("transient failure")
        if len(calls) >= 3:
            recovered.set()
        return 0

    monkeypatch.setattr(publisher, "_drain_once", flaky_drain)
    publisher._settings.outbox_poll_interval = 0.01

    task = asyncio.create_task(publisher._run())
    try:
        # The first tick raises; the loop must survive it and keep ticking.
        async with asyncio.timeout(2):
            await recovered.wait()
    finally:
        publisher._stop_event.set()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
