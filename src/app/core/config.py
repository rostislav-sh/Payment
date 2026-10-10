"""Application settings, split per component.

Each process reads only what it needs: migrations and relay do not require API_KEY,
the API does not require RabbitMQ settings. Values come from environment variables
(and from `.env` in the current working directory for local runs; the project root).

Getters are cached; tests must call ``clear_settings_cache()`` after changing env vars.
"""

from functools import lru_cache
from typing import Literal, Self

from pydantic import Field, PositiveFloat, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class _Base(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


class LoggingSettings(_Base):
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"

    @field_validator("log_level", mode="before")
    @classmethod
    def _upper(cls, value: object) -> object:
        return value.upper() if isinstance(value, str) else value


class DatabaseSettings(_Base):
    database_url: SecretStr


class BrokerSettings(_Base):
    rabbitmq_url: SecretStr
    consumer_prefetch: int = Field(default=10, ge=1)


class ApiSettings(_Base):
    # Printable ASCII without spaces: servers strip header whitespace and decode as latin-1,
    # so any other key could never match.
    api_key: SecretStr = Field(min_length=1, pattern=r"^[!-~]+$")


class RelaySettings(_Base):
    relay_batch_size: int = Field(default=100, ge=1)
    relay_poll_interval: PositiveFloat = 0.5


class ConsumerSettings(_Base):
    webhook_timeout: PositiveFloat = 5.0
    gateway_min_delay: float = Field(default=2.0, ge=0)
    gateway_max_delay: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)

    @model_validator(mode="after")
    def _check_delays(self) -> Self:
        if self.gateway_min_delay > self.gateway_max_delay:
            raise ValueError("GATEWAY_MIN_DELAY must not exceed GATEWAY_MAX_DELAY")
        return self


@lru_cache
def get_logging_settings() -> LoggingSettings:
    return LoggingSettings()


@lru_cache
def get_database_settings() -> DatabaseSettings:
    return DatabaseSettings()  # database_url is required, read from the environment


@lru_cache
def get_broker_settings() -> BrokerSettings:
    return BrokerSettings()


@lru_cache
def get_api_settings() -> ApiSettings:
    return ApiSettings()  # api_key comes from the environment


@lru_cache
def get_relay_settings() -> RelaySettings:
    return RelaySettings()


@lru_cache
def get_consumer_settings() -> ConsumerSettings:
    return ConsumerSettings()


def clear_settings_cache() -> None:
    for getter in (
        get_logging_settings,
        get_database_settings,
        get_broker_settings,
        get_api_settings,
        get_relay_settings,
        get_consumer_settings,
    ):
        getter.cache_clear()
