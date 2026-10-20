from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Overall
    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbit_url: str = "amqp://guest:guest@localhost:5672/"
    api_key: str = "supersecretkey"

    # Outbox
    outbox_poll_interval: float = 1.0  # период опроса таблицы outbox в секундах
    outbox_batch_size: int = 100

    # Consumer
    consumer_prefetch_count: int = 10
    max_retries: int = 3
    retry_base_delay_seconds: int = 2
    webhook_timeout_seconds: float = 10.0

    # Эмуляция платежа
    gateway_min_delay_seconds: float = 2.0
    gateway_max_delay_seconds: float = 5.0
    gateway_success_rate: float = 0.9


@lru_cache
def get_settings() -> Settings:
    return Settings()
