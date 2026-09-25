import pytest

from async_pay.consumer.retry import (
    attempts_exhausted,
    backoff_delay,
    delivery_attempt,
)


class FakeMessage:
    def __init__(self, headers: dict) -> None:
        self.headers = headers


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({}, 1),
        ({"x-delivery-count": 0}, 1),
        ({"x-delivery-count": 1}, 2),
        ({"x-delivery-count": 2}, 3),
        ({"x-delivery-count": "2"}, 3),
        ({"x-delivery-count": None}, 1),
        ({"x-delivery-count": "nonsense"}, 1),
        ({"x-delivery-count": -5}, 1),
    ],
)
def test_delivery_attempt(headers, expected):
    assert delivery_attempt(FakeMessage(headers)) == expected


def test_backoff_delay_is_exponential(settings):
    assert [backoff_delay(a, settings) for a in (1, 2, 3)] == [1.0, 2.0, 4.0]


def test_backoff_delay_scales_with_base(settings):
    settings.consume_retry_base_delay = 0.5
    assert [backoff_delay(a, settings) for a in (1, 2, 3)] == [0.5, 1.0, 2.0]


def test_attempts_exhausted_on_delivery_limit(settings):
    assert settings.max_delivery_count == 3
    assert not attempts_exhausted(1, settings)
    assert not attempts_exhausted(2, settings)
    assert attempts_exhausted(3, settings)
