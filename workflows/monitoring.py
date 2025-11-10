from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional

from .paths import get_s3_client, get_s3_fs, get_storage_settings, s3_uri

logger = logging.getLogger(__name__)

STORAGE_SETTINGS = get_storage_settings()
S3_CLIENT = get_s3_client()
S3_FS = get_s3_fs()


def _latest_modified(bucket: str) -> Optional[datetime]:
    paginator = S3_CLIENT.get_paginator("list_objects_v2")
    latest: Optional[datetime] = None
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            modified = obj["LastModified"].astimezone(timezone.utc)
            if latest is None or modified > latest:
                latest = modified
    return latest


def check_system_health(max_lag_minutes: int = 5) -> Dict[str, Optional[str]]:
    latest_bronze = _latest_modified(STORAGE_SETTINGS.bronze_bucket)
    if latest_bronze is None:
        healthy = False
    else:
        healthy = (datetime.now(timezone.utc) - latest_bronze) <= timedelta(minutes=max_lag_minutes)
    return {
        "healthy": healthy,
        "latest_bronze": latest_bronze.isoformat() if latest_bronze else None,
    }


def _count_objects(bucket: str) -> int:
    paginator = S3_CLIENT.get_paginator("list_objects_v2")
    total = 0
    for page in paginator.paginate(Bucket=bucket):
        total += len(page.get("Contents", []))
    return total


def generate_observability_metrics() -> str:
    metrics = {
        "bronze_files": _count_objects(STORAGE_SETTINGS.bronze_bucket),
        "silver_files": _count_objects(STORAGE_SETTINGS.silver_bucket),
        "gold_files": _count_objects(STORAGE_SETTINGS.gold_bucket),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    metrics_key = "dashboards/pipeline_metrics.json"
    metrics_path = s3_uri(STORAGE_SETTINGS.gold_bucket, metrics_key)
    with S3_FS.open(metrics_path, "w") as handle:
        handle.write(json.dumps(metrics, indent=2))
    return metrics_path


def send_alerts_if_needed(health: Dict[str, Optional[str]], metrics_path: str) -> None:
    alerts_path = Path("logs") / "alerts.log"
    alerts_path.parent.mkdir(exist_ok=True)
    if health.get("healthy"):
        return
    message = (
        f"Pipeline delay detected. Latest bronze event: {health.get('latest_bronze')} "
        f"| metrics_snapshot={metrics_path}"
    )
    with alerts_path.open("a", encoding="utf-8") as handle:
        handle.write(f"{datetime.now(timezone.utc).isoformat()} | {message}\n")
    logger.warning(message)
