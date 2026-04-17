from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    database_url: str = Field(...)
    rabbitmq_url: str = Field(...)
    api_key: str = Field(...)

    log_level: str = "INFO"

    outbox_poll_interval: float = 2.0
    outbox_batch_size: int = 100

    payments_exchange: str = "payments"
    payments_routing_key: str = "payments.new"
    payments_queue: str = "payments.new"
    payments_dlx: str = "payments.dlx"
    payments_dlq: str = "payments.dead"
    payments_dlq_routing_key: str = "payments.dead"

    webhook_timeout_seconds: float = 10.0
    webhook_max_attempts: int = 3

    processing_min_seconds: float = 2.0
    processing_max_seconds: float = 5.0
    success_probability: float = 0.9

    max_delivery_count: int = 3


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
