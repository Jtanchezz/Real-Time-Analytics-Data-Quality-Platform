from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_name: str = "Bike Trips API"
    storage_mode: Literal["local", "s3"] = "s3"
    bronze_path: Path = Path("data/bronze")
    bronze_bucket: str = "bronze"
    silver_bucket: str = "silver"
    gold_bucket: str = "gold"
    historical_bucket: str = "historical"
    s3_endpoint_url: str = "http://localhost:9000"
    s3_region: str = "us-east-1"
    s3_access_key: str = "admin"
    s3_secret_key: str = "admin123"
    metrics_window_seconds: int = 60
    max_events_per_second: int = 10
    timezone: str = "UTC"
    log_level: str = "INFO"

    class Config:
        env_prefix = "APP_"
        env_file = ".env"
        env_file_encoding = "utf-8"

    @property
    def storage_options(self) -> dict:
        return {
            "key": self.s3_access_key,
            "secret": self.s3_secret_key,
            "client_kwargs": {
                "endpoint_url": self.s3_endpoint_url,
                "region_name": self.s3_region,
            },
        }


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    if settings.storage_mode == "local":
        settings.bronze_path.mkdir(parents=True, exist_ok=True)
    return settings
