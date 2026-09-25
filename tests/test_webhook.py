from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest

from async_pay.consumer import handler as handler_module
from async_pay.consumer import webhook
from async_pay.models import Payment, PaymentStatus
from async_pay.schemas import WebhookPayload

HOOK_URL = "https://example.com/hook"


def _payload() -> WebhookPayload:
    return WebhookPayload(
        payment_id=uuid4(),
        status=PaymentStatus.SUCCEEDED,
        processed_at=datetime.now(UTC),
    )


async def _deliver(client: httpx.AsyncClient, **kwargs) -> bool:
    # retry_base_delay=0 everywhere so the suite never actually sleeps.
    kwargs.setdefault("retry_base_delay", 0.0)
    return await webhook.deliver_webhook(HOOK_URL, _payload(), client=client, **kwargs)


async def test_webhook_success_on_first_attempt():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        assert await _deliver(client) is True


async def test_webhook_retries_then_gives_up():
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        assert await _deliver(client) is False
    assert calls == webhook.DEFAULT_MAX_ATTEMPTS


@pytest.mark.parametrize("fail_until", [1, 2])
async def test_webhook_recovers_after_failures(fail_until):
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls <= fail_until:
            return httpx.Response(503)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        assert await _deliver(client) is True
    assert calls == fail_until + 1


async def test_webhook_honours_max_attempts():
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        assert await _deliver(client, max_attempts=5) is False
    assert calls == 5


def test_retry_delay_is_exponential():
    assert [webhook.retry_delay(a) for a in (1, 2, 3)] == [1.0, 2.0, 4.0]
    assert [webhook.retry_delay(a, 0.5) for a in (1, 2, 3)] == [0.5, 1.0, 2.0]


async def test_handler_passes_configured_retry_limits(settings, session_factory, monkeypatch):
    """The handler must use the configured limits, not the module defaults."""
    settings.webhook_max_attempts = 5
    settings.webhook_retry_base_delay = 0.25
    settings.webhook_timeout_seconds = 3.0
    settings.processing_min_seconds = 0.0
    settings.processing_max_seconds = 0.0

    captured: dict = {}

    async def fake_deliver(url, payload, **kwargs):
        captured["url"] = url
        captured.update(kwargs)
        return True

    monkeypatch.setattr(handler_module, "deliver_webhook", fake_deliver)

    async with session_factory() as session:
        payment = Payment(
            amount="10.00",
            currency="RUB",
            meta={},
            idempotency_key="key-webhook-settings",
            webhook_url=HOOK_URL,
        )
        session.add(payment)
        await session.commit()
        payment_id = str(payment.id)

    await handler_module.process_payment_event(
        {"payment_id": payment_id}, session_factory, settings
    )

    assert captured["url"] == HOOK_URL
    assert captured["max_attempts"] == 5
    assert captured["retry_base_delay"] == 0.25
    assert captured["timeout"] == 3.0
