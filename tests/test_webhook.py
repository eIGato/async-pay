from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest

from async_pay.consumer import webhook
from async_pay.models import PaymentStatus
from async_pay.schemas import WebhookPayload


def _payload() -> WebhookPayload:
    return WebhookPayload(
        payment_id=uuid4(),
        status=PaymentStatus.SUCCEEDED,
        processed_at=datetime.now(UTC),
    )


async def test_webhook_success_on_first_attempt(monkeypatch):
    monkeypatch.setattr(webhook, "RETRY_DELAYS", (0.0, 0.0, 0.0))

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        ok = await webhook.deliver_webhook(
            "https://example.com/hook", _payload(), client=client
        )
    assert ok is True


async def test_webhook_retries_then_gives_up(monkeypatch):
    monkeypatch.setattr(webhook, "RETRY_DELAYS", (0.0, 0.0, 0.0))
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        ok = await webhook.deliver_webhook(
            "https://example.com/hook", _payload(), client=client
        )
    assert ok is False
    assert calls == webhook.MAX_ATTEMPTS


@pytest.mark.parametrize("fail_until", [1, 2])
async def test_webhook_recovers_after_failures(monkeypatch, fail_until):
    monkeypatch.setattr(webhook, "RETRY_DELAYS", (0.0, 0.0, 0.0))
    counter = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        counter["n"] += 1
        if counter["n"] <= fail_until:
            return httpx.Response(503)
        return httpx.Response(200)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        ok = await webhook.deliver_webhook(
            "https://example.com/hook", _payload(), client=client
        )
    assert ok is True
    assert counter["n"] == fail_until + 1
