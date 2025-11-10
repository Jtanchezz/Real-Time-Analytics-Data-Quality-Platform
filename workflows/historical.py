from __future__ import annotations

from pathlib import PurePosixPath
from typing import Dict, List

import pandas as pd

from .paths import (
    get_s3_client,
    get_storage_settings,
    parse_s3_uri,
    s3_uri,
)
from .quality import promote_to_silver, score_dataset, update_gold_tables

STORAGE_SETTINGS = get_storage_settings()
S3_CLIENT = get_s3_client()
S3_OPTIONS = STORAGE_SETTINGS.storage_options()

PENDING_PREFIX = "pending/"
PROCESSED_PREFIX = "processed/"


def _pending_batches() -> List[str]:
    paginator = S3_CLIENT.get_paginator("list_objects_v2")
    files: List[str] = []
    for page in paginator.paginate(
        Bucket=STORAGE_SETTINGS.historical_bucket, Prefix=PENDING_PREFIX
    ):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                files.append(s3_uri(STORAGE_SETTINGS.historical_bucket, obj["Key"]))
    return sorted(files)


def validate_historical_batches() -> Dict[str, List[str]]:
    valid: List[str] = []
    invalid: List[str] = []
    for batch in _pending_batches():
        try:
            df = pd.read_parquet(batch, storage_options=S3_OPTIONS)
        except Exception:
            invalid.append(batch)
            continue
        missing_columns = [
            col for col in {"trip_id", "start_time", "end_time"} if col not in df.columns
        ]
        if missing_columns:
            invalid.append(batch)
            continue
        valid.append(batch)
    return {"valid_files": valid, "invalid_files": invalid}


def _move_to_processed(uri: str) -> None:
    bucket, key = parse_s3_uri(uri)
    source = {"Bucket": bucket, "Key": key}
    processed_key = key.replace(PENDING_PREFIX, PROCESSED_PREFIX, 1)
    S3_CLIENT.copy_object(Bucket=bucket, CopySource=source, Key=processed_key)
    S3_CLIENT.delete_object(Bucket=bucket, Key=key)


def load_historical_to_bronze(valid_files: List[str]) -> List[str]:
    bronze_paths: List[str] = []
    for uri in valid_files:
        df = pd.read_parquet(uri, storage_options=S3_OPTIONS)
        if df.empty:
            _move_to_processed(uri)
            continue
        df["start_time"] = pd.to_datetime(df["start_time"], utc=True)
        bucket, key = parse_s3_uri(uri)
        stem = PurePosixPath(key).stem
        bronze_key = f"historical/{stem}/{stem}.parquet"
        bronze_uri = s3_uri(STORAGE_SETTINGS.bronze_bucket, bronze_key)
        df.to_parquet(bronze_uri, index=False, storage_options=S3_OPTIONS)
        bronze_paths.append(bronze_uri)
        _move_to_processed(uri)
    return bronze_paths


def process_historical_to_silver(bronze_files: List[str]) -> Dict[str, str]:
    staging_path, metrics = score_dataset(bronze_files)
    if not staging_path:
        return {"silver_path": "", **metrics}
    silver_path = promote_to_silver(staging_path)
    gold_outputs = update_gold_tables(silver_path) if silver_path else {}
    return {
        "silver_path": silver_path,
        **metrics,
        "gold_outputs": gold_outputs,
    }
