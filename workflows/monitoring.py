from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import httpx

from .paths import get_s3_client, get_s3_fs, get_storage_settings, s3_uri

logger = logging.getLogger(__name__)

STORAGE_SETTINGS = get_storage_settings()
S3_CLIENT = get_s3_client()
S3_FS = get_s3_fs()
API_BASE_URL = os.getenv("APP_METRICS_API_URL", "http://localhost:8000").rstrip("/")
FLOW_STATUS_PREFIX = "dashboards/flow_status/"
QUALITY_REPORT_PREFIX = "reports/"
MAX_POOR_THRESHOLD = 0.2


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


def _fetch_api_metrics() -> Dict[str, object]:
    endpoint = f"{API_BASE_URL}/api/v1/metrics"
    try:
        response = httpx.get(endpoint, timeout=5.0)
        response.raise_for_status()
        payload = response.json()
        payload["reachable"] = True
    except Exception as exc:  # pragma: no cover - network failures
        logger.warning("API metrics unavailable: %s", exc)
        payload = {
            "events_per_minute": 0,
            "errors_per_minute": 0,
            "avg_latency_ms": 0.0,
            "reachable": False,
        }
    payload["endpoint"] = endpoint
    return payload


def record_flow_status(flow_name: str, payload: Dict[str, object]) -> str:
    record = {
        "flow": flow_name,
        "status": payload.get("status", "unknown"),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "details": payload,
    }
    key = f"{FLOW_STATUS_PREFIX}{flow_name}.json"
    path = s3_uri(STORAGE_SETTINGS.gold_bucket, key)
    with S3_FS.open(path, "w") as handle:
        handle.write(json.dumps(record, indent=2))
    return path


def _latest_quality_report() -> Dict[str, object]:
    paginator = S3_CLIENT.get_paginator("list_objects_v2")
    latest_obj: Optional[Dict[str, object]] = None
    latest_time: Optional[datetime] = None
    for page in paginator.paginate(
        Bucket=STORAGE_SETTINGS.gold_bucket, Prefix=QUALITY_REPORT_PREFIX
    ):
        for obj in page.get("Contents", []):
            modified = obj["LastModified"].astimezone(timezone.utc)
            if latest_time is None or modified > latest_time:
                latest_time = modified
                latest_obj = obj
    if not latest_obj:
        return {}
    report_path = s3_uri(STORAGE_SETTINGS.gold_bucket, latest_obj["Key"])
    try:
        with S3_FS.open(report_path, "r") as handle:
            payload = json.load(handle)
            payload["report_path"] = report_path
            return payload
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to load quality report: %s", exc)
        return {}


def _list_flow_statuses() -> List[Dict[str, object]]:
    paginator = S3_CLIENT.get_paginator("list_objects_v2")
    statuses: List[Dict[str, object]] = []
    for page in paginator.paginate(
        Bucket=STORAGE_SETTINGS.gold_bucket, Prefix=FLOW_STATUS_PREFIX
    ):
        for obj in page.get("Contents", []):
            path = s3_uri(STORAGE_SETTINGS.gold_bucket, obj["Key"])
            try:
                with S3_FS.open(path, "r") as handle:
                    payload = json.load(handle)
                    payload["path"] = path
                    statuses.append(payload)
            except Exception:
                continue
    return statuses


def generate_observability_metrics() -> Dict[str, object]:
    api_metrics = _fetch_api_metrics()
    quality_report = _latest_quality_report()
    flow_statuses = _list_flow_statuses()
    metrics = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "api": api_metrics,
        "quality": quality_report,
        "flows": flow_statuses,
        "storage": {
            "bronze_files": _count_objects(STORAGE_SETTINGS.bronze_bucket),
            "silver_files": _count_objects(STORAGE_SETTINGS.silver_bucket),
            "gold_files": _count_objects(STORAGE_SETTINGS.gold_bucket),
        },
    }
    metrics_key = "dashboards/pipeline_metrics.json"
    metrics_path = s3_uri(STORAGE_SETTINGS.gold_bucket, metrics_key)
    with S3_FS.open(metrics_path, "w") as handle:
        handle.write(json.dumps(metrics, indent=2))
    return {"path": metrics_path, "payload": metrics}


def send_alerts_if_needed(
    health: Dict[str, Optional[str]], metrics_snapshot: Dict[str, object]
) -> None:
    alerts: List[str] = []
    payload = metrics_snapshot.get("payload", {})
    metrics_path = metrics_snapshot.get("path")

    api_metrics = payload.get("api", {})
    if not api_metrics.get("reachable"):
        alerts.append("API metrics endpoint unreachable")

    quality = payload.get("quality", {})
    poor_share = quality.get("poor_share", 0)
    if poor_share and poor_share > MAX_POOR_THRESHOLD:
        alerts.append(f"Poor quality share at {poor_share:.2%}")

    for flow in payload.get("flows", []):
        if flow.get("status") not in {"completed", "empty"}:
            alerts.append(f"Flow {flow.get('flow')} status={flow.get('status')}")

    if not health.get("healthy"):
        alerts.append(
            f"Pipeline delay detected. Latest bronze event: {health.get('latest_bronze')}"
        )

    if not alerts:
        return

    alerts_path = Path("logs") / "alerts.log"
    alerts_path.parent.mkdir(exist_ok=True)
    for message in alerts:
        log_message = f"{datetime.now(timezone.utc).isoformat()} | {message} | snapshot={metrics_path}"
        with alerts_path.open("a", encoding="utf-8") as handle:
            handle.write(f"{log_message}\n")
        logger.warning(log_message)
