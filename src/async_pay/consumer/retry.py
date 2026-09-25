import logging

from faststream.rabbit import RabbitMessage

from async_pay.config import Settings

logger = logging.getLogger(__name__)

DELIVERY_COUNT_HEADER = "x-delivery-count"


def delivery_attempt(message: RabbitMessage) -> int:
    """Return the 1-based delivery attempt number for a message.

    Quorum queues stamp every redelivery with ``x-delivery-count`` (absent on
    the first delivery, ``1`` on the second, and so on), so the current attempt
    is that counter plus one.
    """
    raw = message.headers.get(DELIVERY_COUNT_HEADER, 0)
    try:
        count = int(raw)
    except (TypeError, ValueError):
        logger.warning("unparsable %s header: %r", DELIVERY_COUNT_HEADER, raw)
        count = 0
    return max(count, 0) + 1


def backoff_delay(attempt: int, settings: Settings) -> float:
    """Exponential backoff for the *next* delivery: base, base*2, base*4, ..."""
    exponent = max(attempt - 1, 0)
    return settings.consume_retry_base_delay * (2.0**exponent)


def attempts_exhausted(attempt: int, settings: Settings) -> bool:
    return attempt >= settings.max_delivery_count
