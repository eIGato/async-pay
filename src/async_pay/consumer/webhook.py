import asyncio
import logging
from typing import Final

import httpx

from async_pay.schemas import WebhookPayload

logger = logging.getLogger(__name__)

RETRY_DELAYS: Final[tuple[float, ...]] = (1.0, 2.0, 4.0)
MAX_ATTEMPTS: Final[int] = 3


async def deliver_webhook(
    url: str,
    payload: WebhookPayload,
    *,
    timeout: float = 10.0,
    client: httpx.AsyncClient | None = None,
) -> bool:
    """Deliver webhook with exponential backoff retry. Returns True on success."""
    body = payload.model_dump(mode="json")
    owned_client = client is None
    http = client or httpx.AsyncClient(timeout=timeout)
    try:
        for attempt in range(1, MAX_ATTEMPTS + 1):
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
                    "webhook attempt %d failed payment_id=%s url=%s: %s",
                    attempt,
                    payload.payment_id,
                    url,
                    exc,
                )
                if attempt < MAX_ATTEMPTS:
                    await asyncio.sleep(RETRY_DELAYS[attempt - 1])
        logger.error(
            "webhook delivery gave up after %d attempts payment_id=%s url=%s",
            MAX_ATTEMPTS,
            payload.payment_id,
            url,
        )
        return False
    finally:
        if owned_client:
            await http.aclose()
