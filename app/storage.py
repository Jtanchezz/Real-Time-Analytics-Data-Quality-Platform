from __future__ import annotations

import io
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from .config import Settings


class BronzeWriter:
    """Persist events as Parquet files partitioned by date/hour."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.mode = settings.storage_mode
        if self.mode == "local":
            self.base_path = settings.bronze_path
            self.base_path.mkdir(parents=True, exist_ok=True)
            self.s3_client = None
        else:
            self.base_path = None
            self.s3_client = boto3.client(
                "s3",
                endpoint_url=settings.s3_endpoint_url,
                region_name=settings.s3_region,
                aws_access_key_id=settings.s3_access_key,
                aws_secret_access_key=settings.s3_secret_key,
            )

    def _partition_components(self, event_time: datetime) -> Tuple[str, str]:
        date_part = event_time.strftime("date=%Y-%m-%d")
        hour_part = event_time.strftime("hour=%H")
        return date_part, hour_part

    def persist_event(self, event_payload: Dict[str, Any]) -> None:
        logger = logging.getLogger(__name__)
        start_time_str = event_payload["start_time"]
        if start_time_str.endswith("Z"):
            start_time_str = start_time_str.replace("Z", "+00:00")
        event_time = datetime.fromisoformat(start_time_str)
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)
        event_time = event_time.astimezone(timezone.utc)
        date_part, hour_part = self._partition_components(event_time)
        event_payload = {
            **event_payload,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }
        table = pa.Table.from_pylist([event_payload])
        parquet_name = f"event_{uuid.uuid4().hex}.parquet"
        json_name = f"event_{uuid.uuid4().hex}.json"
        try:
            if self.mode == "local":
                assert self.base_path is not None
                partition_dir = self.base_path / date_part / hour_part
                partition_dir.mkdir(parents=True, exist_ok=True)
                pq.write_table(table, partition_dir / parquet_name, compression="snappy")
                (partition_dir / json_name).write_text(
                    json.dumps(event_payload), encoding="utf-8"
                )
            else:
                assert self.s3_client is not None
                key_prefix = f"{date_part}/{hour_part}"
                parquet_buffer = io.BytesIO()
                pq.write_table(table, parquet_buffer, compression="snappy")
                parquet_buffer.seek(0)
                self.s3_client.put_object(
                    Bucket=self.settings.bronze_bucket,
                    Key=f"{key_prefix}/{parquet_name}",
                    Body=parquet_buffer.read(),
                    ContentType="application/octet-stream",
                )
                self.s3_client.put_object(
                    Bucket=self.settings.bronze_bucket,
                    Key=f"{key_prefix}/{json_name}",
                    Body=json.dumps(event_payload).encode("utf-8"),
                    ContentType="application/json",
                )
        except Exception:  # pragma: no cover - defensive logging
            logger.exception(
                "Failed to persist event to bronze",
                extra={"trip_id": event_payload.get("trip_id")},
            )
            raise
