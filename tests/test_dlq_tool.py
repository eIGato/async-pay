import json

import pytest

from async_pay.tools import dlq as dlq_tool


class FakeMessage:
    def __init__(self, body: bytes, message_id: str = "mid") -> None:
        self.body = body
        self.message_id = message_id
        self.headers: dict = {}
        self.nacked_requeue: bool | None = None

    async def nack(self, requeue: bool = False) -> None:
        self.nacked_requeue = requeue


class FakeQueue:
    def __init__(self, messages: list[FakeMessage]) -> None:
        self._pending = list(messages)
        self.get_calls = 0

    async def get(self, *, no_ack: bool = False, fail: bool = True):
        self.get_calls += 1
        if not self._pending:
            return None
        return self._pending.pop(0)


class FakeChannel:
    def __init__(self, queue: FakeQueue) -> None:
        self._queue = queue

    async def get_queue(self, name: str, ensure: bool = True) -> FakeQueue:
        return self._queue


class FakeConnection:
    def __init__(self, queue: FakeQueue) -> None:
        self._queue = queue
        self.closed = False

    async def channel(self) -> FakeChannel:
        return FakeChannel(self._queue)

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def messages() -> list[FakeMessage]:
    return [
        FakeMessage(json.dumps({"payment_id": f"p{i}"}).encode(), message_id=f"m{i}")
        for i in range(3)
    ]


@pytest.fixture
def patched_connect(monkeypatch, messages):
    queue = FakeQueue(messages)
    connection = FakeConnection(queue)

    async def fake_connect(url: str):
        return connection

    monkeypatch.setattr(dlq_tool.aio_pika, "connect_robust", fake_connect)
    return connection, queue


async def test_peek_returns_every_message_to_the_queue(settings, patched_connect, messages, capsys):
    _, queue = patched_connect

    seen = await dlq_tool.peek(settings, limit=10)

    assert seen == 3
    # Nothing is consumed: every message goes back with requeue=True.
    assert [m.nacked_requeue for m in messages] == [True, True, True]
    out = capsys.readouterr().out
    assert "p0" in out and "p2" in out
    assert "3 message(s) shown" in out
    # One extra get to discover the queue is drained, no endless re-reads.
    assert queue.get_calls == 4


async def test_peek_honours_limit(settings, patched_connect, messages):
    seen = await dlq_tool.peek(settings, limit=2)

    assert seen == 2
    assert [m.nacked_requeue for m in messages] == [True, True, None]


async def test_peek_reports_empty_queue(settings, monkeypatch, capsys):
    queue = FakeQueue([])
    connection = FakeConnection(queue)

    async def fake_connect(url: str):
        return connection

    monkeypatch.setattr(dlq_tool.aio_pika, "connect_robust", fake_connect)

    assert await dlq_tool.peek(settings) == 0
    assert "is empty" in capsys.readouterr().out
    assert connection.closed


def test_format_body_falls_back_to_repr():
    assert dlq_tool._format_body(b"\xff\xfe") == repr(b"\xff\xfe")
    assert dlq_tool._format_body(b'{"b":1,"a":2}') == '{"a": 2, "b": 1}'
