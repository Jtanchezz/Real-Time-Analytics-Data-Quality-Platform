from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Tuple

import boto3
import s3fs


@dataclass
class StorageSettings:
    endpoint_url: str = os.getenv("APP_S3_ENDPOINT_URL", "http://localhost:9000")
    access_key: str = os.getenv("APP_S3_ACCESS_KEY", "admin")
    secret_key: str = os.getenv("APP_S3_SECRET_KEY", "admin123")
    region: str = os.getenv("APP_S3_REGION", "us-east-1")
    bronze_bucket: str = os.getenv("APP_BRONZE_BUCKET", "bronze")
    silver_bucket: str = os.getenv("APP_SILVER_BUCKET", "silver")
    gold_bucket: str = os.getenv("APP_GOLD_BUCKET", "gold")
    historical_bucket: str = os.getenv("APP_HISTORICAL_BUCKET", "historical")
    reference_dir: Path = Path(os.getenv("REFERENCE_DIR", "reference"))

    def storage_options(self) -> dict:
        return {
            "key": self.access_key,
            "secret": self.secret_key,
            "client_kwargs": {
                "endpoint_url": self.endpoint_url,
                "region_name": self.region,
            },
        }


@lru_cache
def get_storage_settings() -> StorageSettings:
    return StorageSettings()


@lru_cache
def get_s3_client():
    settings = get_storage_settings()
    return boto3.client(
        "s3",
        endpoint_url=settings.endpoint_url,
        region_name=settings.region,
        aws_access_key_id=settings.access_key,
        aws_secret_access_key=settings.secret_key,
    )


@lru_cache
def get_s3_fs() -> s3fs.S3FileSystem:
    settings = get_storage_settings()
    return s3fs.S3FileSystem(
        client_kwargs={
            "endpoint_url": settings.endpoint_url,
            "region_name": settings.region,
        },
        key=settings.access_key,
        secret=settings.secret_key,
    )


def s3_uri(bucket: str, key: str = "") -> str:
    clean = key.lstrip("/")
    return f"s3://{bucket}/{clean}" if clean else f"s3://{bucket}"


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri}")
    bucket, _, key = uri[5:].partition("/")
    return bucket, key


REFERENCE_DIR = get_storage_settings().reference_dir
