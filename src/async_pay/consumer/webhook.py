import asyncio
import logging

import httpx

from async_pay.schemas import WebhookPayload

logger = logging.getLogger(__name__)

DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_RETRY_BASE_DELAY = 1.0


def retry_delay(attempt: int, base_delay: float = DEFAULT_RETRY_BASE_DELAY) -> float:
    """Backoff before the attempt following ``attempt``: base, base*2, base*4, ..."""
    return base_delay * (2.0 ** max(attempt - 1, 0))


async def deliver_webhook(
    url: str,
    payload: WebhookPayload,
    *,
    timeout: float = 10.0,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    retry_base_delay: float = DEFAULT_RETRY_BASE_DELAY,
    client: httpx.AsyncClient | None = None,
) -> bool:
    """Deliver webhook with exponential backoff retry. Returns True on success."""
    body = payload.model_dump(mode="json")
    owned_client = client is None
    http = client or httpx.AsyncClient(timeout=timeout)
    try:
        for attempt in range(1, max_attempts + 1):
            try:
                response = await http.post(url, json=body)
                response.raise_for_status()
                logger.info(
                    "webhook delivered payment_id=%s url=%s attempt=%d",
                    payload.payment_id,
                    url,
                    attempt,
                )
                return True
            except (httpx.HTTPError, httpx.TimeoutException) as exc:
                logger.warning(
                    "webhook attempt %d/%d failed payment_id=%s url=%s: %s",
                    attempt,
                    max_attempts,
                    payload.payment_id,
                    url,
                    exc,
                )
                if attempt < max_attempts:
                    await asyncio.sleep(retry_delay(attempt, retry_base_delay))
        logger.error(
            "webhook delivery gave up after %d attempts payment_id=%s url=%s",
            max_attempts,
            payload.payment_id,
            url,
        )
        return False
    finally:
        if owned_client:
            await http.aclose()
