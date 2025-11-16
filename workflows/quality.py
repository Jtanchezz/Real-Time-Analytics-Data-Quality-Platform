from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from pandas import DataFrame

from .paths import (
    REFERENCE_DIR,
    get_s3_client,
    get_s3_fs,
    get_storage_settings,
    parse_s3_uri,
    s3_uri,
)

BASE_SCORE = 100
REQUIRED_FIELDS = [
    "trip_id",
    "bike_id",
    "start_time",
    "end_time",
    "start_station_id",
    "end_station_id",
    "rider_age",
    "trip_duration",
    "bike_type",
    "member_casual",
]
ALLOWED_FIELDS = REQUIRED_FIELDS + ["ingested_at", "source_type"]
MAX_SPEED_KMH = 30
MIN_TRIP_DURATION = 60
MAX_TRIP_DURATION = 86_400  # 24 hours
REALTIME_CHECKPOINT_KEY = "control/realtime_checkpoint.json"
SCHEMA_MAX_DEDUCTION = 40
VALIDITY_MAX_DEDUCTION = 40
BUSINESS_MAX_DEDUCTION = 20
STRING_FIELDS = {"trip_id", "bike_type", "member_casual"}
NUMERIC_FIELDS = {
    "bike_id",
    "start_station_id",
    "end_station_id",
    "rider_age",
    "trip_duration",
}
DATETIME_FIELDS = {"start_time", "end_time"}

STORAGE_SETTINGS = get_storage_settings()
S3_CLIENT = get_s3_client()
S3_FS = get_s3_fs()
S3_OPTIONS = STORAGE_SETTINGS.storage_options()
STATION_LOOKUP_PATH = REFERENCE_DIR / "stations_reference.csv"
REALTIME_CHECKPOINT_URI = s3_uri(STORAGE_SETTINGS.gold_bucket, REALTIME_CHECKPOINT_KEY)


def _load_realtime_checkpoint() -> Tuple[Optional[datetime], str]:
    try:
        with S3_FS.open(REALTIME_CHECKPOINT_URI, "r") as handle:
            payload = json.load(handle)
    except FileNotFoundError:
        return None, ""
    except Exception:
        return None, ""
    timestamp = payload.get("last_modified")
    last_key = payload.get("last_key", "")
    if not timestamp:
        return None, last_key
    try:
        last_modified = datetime.fromisoformat(timestamp)
        if last_modified.tzinfo is None:
            last_modified = last_modified.replace(tzinfo=timezone.utc)
    except ValueError:
        return None, last_key
    return last_modified, last_key


def _persist_realtime_checkpoint(last_modified: datetime, last_key: str) -> None:
    if last_modified.tzinfo is None:
        last_modified = last_modified.replace(tzinfo=timezone.utc)
    payload = {
        "last_modified": last_modified.isoformat(),
        "last_key": last_key,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    with S3_FS.open(REALTIME_CHECKPOINT_URI, "w") as handle:
        json.dump(payload, handle)


def discover_new_data(window_minutes: int = 15) -> List[str]:
    checkpoint_time, checkpoint_key = _load_realtime_checkpoint()
    lookback_cutoff = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
    paginator = S3_CLIENT.get_paginator("list_objects_v2")
    discovered: List[Tuple[datetime, str]] = []
    for page in paginator.paginate(Bucket=STORAGE_SETTINGS.bronze_bucket):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet") or key.startswith("historical/"):
                continue
            modified = obj["LastModified"].astimezone(timezone.utc)
            is_new = False
            if checkpoint_time:
                is_new = modified > checkpoint_time or (
                    modified == checkpoint_time and key > checkpoint_key
                )
            else:
                is_new = True
            if is_new:
                discovered.append((modified, key))

    discovered.sort(key=lambda item: (item[0], item[1]))
    files = [
        s3_uri(STORAGE_SETTINGS.bronze_bucket, key)
        for _, key in discovered
    ]
    if discovered:
        latest_modified, latest_key = discovered[-1]
        _persist_realtime_checkpoint(latest_modified, latest_key)
    return files


def _load_station_lookup() -> Dict[int, Tuple[float, float]]:
    if not STATION_LOOKUP_PATH.exists():
        return {}
    df = pd.read_csv(STATION_LOOKUP_PATH)
    return {
        int(row.station_id): (float(row.lat), float(row.lon))
        for row in df.itertuples()
    }


def _load_frames(files: List[str]) -> DataFrame:
    frames: List[DataFrame] = []
    for file_path in files:
        if file_path.endswith(".parquet"):
            frames.append(pd.read_parquet(file_path, storage_options=S3_OPTIONS))
        elif file_path.endswith(".json"):
            frames.append(pd.read_json(file_path, storage_options=S3_OPTIONS))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _is_missing(value) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _parse_datetime(value) -> Tuple[Optional[pd.Timestamp], bool]:
    if _is_missing(value):
        return None, False
    try:
        parsed = pd.to_datetime(value, utc=True)
    except Exception:
        return None, True
    return parsed, False


def _coerce_number(value) -> Tuple[Optional[float], bool]:
    if _is_missing(value):
        return None, False
    try:
        coerced = float(value)
    except (TypeError, ValueError):
        return None, True
    return coerced, False


def _require_string(value) -> bool:
    if _is_missing(value):
        return False
    return not isinstance(value, str)


def run_comprehensive_checks(files: List[str]) -> Dict[str, int]:
    df = _load_frames(files)
    if df.empty:
        return {"records": 0, "issues_found": 0}

    present_cols = [col for col in REQUIRED_FIELDS if col in df.columns]
    missing_in_rows = df[present_cols].isna().sum().sum() if present_cols else 0
    missing_columns_penalty = (len(REQUIRED_FIELDS) - len(present_cols)) * len(df)
    missing_values = missing_in_rows + missing_columns_penalty
    duplicates = df.duplicated(subset=["trip_id"]).sum()
    negative_durations = (
        df["trip_duration"].fillna(0) <= 0
        if "trip_duration" in df.columns
        else pd.Series(dtype=int)
    )
    negative_count = (
        int(negative_durations.sum()) if hasattr(negative_durations, "sum") else 0
    )

    return {
        "records": int(len(df)),
        "issues_found": int(missing_values + duplicates + negative_count),
        "missing_values": int(missing_values),
        "duplicate_ids": int(duplicates),
        "non_positive_duration": int(negative_count),
    }


def _haversine(p1: Tuple[float, float], p2: Tuple[float, float]) -> float:
    lat1, lon1 = map(math.radians, p1)
    lat2, lon2 = map(math.radians, p2)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    earth_radius_km = 6371
    return earth_radius_km * c


def _estimate_speed_kmh(row: pd.Series, stations: Dict[int, Tuple[float, float]]) -> Optional[float]:
    def _coord(field: str) -> Optional[Tuple[float, float]]:
        value = row.get(field)
        if value is None or pd.isna(value):
            return None
        try:
            station_id = int(value)
        except (TypeError, ValueError):
            return None
        return stations.get(station_id)

    start_station = _coord("start_station_id")
    end_station = _coord("end_station_id")
    if not start_station or not end_station:
        return None
    duration = row.get("trip_duration")
    if not duration or duration <= 0:
        return None
    distance = _haversine(start_station, end_station)
    hours = duration / 3600
    if hours == 0:
        return None
    return distance / hours


def _score_band(score: float) -> str:
    if score >= 90:
        return "EXCELLENT"
    if score >= 75:
        return "GOOD"
    if score >= 60:
        return "FAIR"
    return "POOR"


def score_dataset(
    files: List[str], required_source_type: Optional[str] = None
) -> Tuple[Optional[str], Dict[str, float]]:
    df = _load_frames(files)
    if df.empty:
        return None, {"records": 0, "quality_pass_rate": 0.0, "poor_share": 0.0}

    if required_source_type:
        if "source_type" not in df.columns:
            df["source_type"] = required_source_type
        mask = (
            df["source_type"]
            .fillna("")
            .str.lower()
            .eq(required_source_type.lower())
        )
        df = df.loc[mask].reset_index(drop=True)
        if df.empty:
            return None, {"records": 0, "quality_pass_rate": 0.0, "poor_share": 0.0}

    stations = _load_station_lookup()

    def _score_row(row: pd.Series) -> Tuple[float, float, str, float, float, float]:
        schema_penalty = 0.0
        validity_penalty = 0.0
        business_penalty = 0.0

        missing_fields = [field for field in REQUIRED_FIELDS if _is_missing(row.get(field))]
        schema_penalty += 10 * len(missing_fields)

        start_dt: Optional[pd.Timestamp] = None
        end_dt: Optional[pd.Timestamp] = None
        duration_value: Optional[float] = None
        rider_age_value: Optional[float] = None
        start_station_value: Optional[float] = None
        end_station_value: Optional[float] = None

        invalid_fields: List[str] = []
        for field in REQUIRED_FIELDS:
            if field in missing_fields:
                continue
            value = row.get(field)
            if field in STRING_FIELDS:
                if _require_string(value):
                    invalid_fields.append(field)
            elif field in DATETIME_FIELDS:
                parsed, invalid = _parse_datetime(value)
                if invalid:
                    invalid_fields.append(field)
                if field == "start_time":
                    start_dt = parsed
                else:
                    end_dt = parsed
            elif field in NUMERIC_FIELDS:
                coerced, invalid = _coerce_number(value)
                if invalid:
                    invalid_fields.append(field)
                if field == "trip_duration":
                    duration_value = coerced
                elif field == "rider_age":
                    rider_age_value = coerced
                elif field == "start_station_id":
                    start_station_value = coerced
                elif field == "end_station_id":
                    end_station_value = coerced
        schema_penalty += 5 * len(invalid_fields)

        unexpected_fields = [field for field in row.index if field not in ALLOWED_FIELDS]
        schema_penalty += 2 * len(unexpected_fields)
        schema_penalty = min(schema_penalty, SCHEMA_MAX_DEDUCTION)

        if start_dt is not None and end_dt is not None:
            if start_dt >= end_dt:
                validity_penalty += 25
            duration_delta = (end_dt - start_dt).total_seconds()
            if duration_value is None:
                duration_value = duration_delta
            else:
                if abs(duration_delta - duration_value) > 60:
                    validity_penalty += 15
        else:
            duration_delta = None

        if rider_age_value is not None:
            if not (16 <= rider_age_value <= 100):
                validity_penalty += 20

        def _penalize_station(value: Optional[float]) -> None:
            nonlocal validity_penalty
            if value is None:
                return
            try:
                station_id = int(value)
            except (TypeError, ValueError):
                validity_penalty += 10
                return
            if stations and station_id not in stations:
                validity_penalty += 10

        _penalize_station(start_station_value)
        _penalize_station(end_station_value)
        validity_penalty = min(validity_penalty, VALIDITY_MAX_DEDUCTION)

        if duration_value is not None:
            if duration_value < MIN_TRIP_DURATION or duration_value > MAX_TRIP_DURATION:
                business_penalty += 15
            if (
                start_station_value is not None
                and end_station_value is not None
                and int(start_station_value) == int(end_station_value)
                and duration_value > 3600
            ):
                business_penalty += 5

        speed = _estimate_speed_kmh(row, stations)
        if speed and speed > MAX_SPEED_KMH:
            business_penalty += 10
        business_penalty = min(business_penalty, BUSINESS_MAX_DEDUCTION)

        total_deductions = schema_penalty + validity_penalty + business_penalty
        normalized_score = max(BASE_SCORE - total_deductions, 0) / BASE_SCORE
        band = _score_band(normalized_score * 100)
        return (
            round(normalized_score, 4),
            round(total_deductions, 2),
            band,
            round(schema_penalty, 2),
            round(validity_penalty, 2),
            round(business_penalty, 2),
        )

    scores = df.apply(
        lambda r: pd.Series(
            _score_row(r),
            index=[
                "quality_score",
                "deductions",
                "quality_band",
                "schema_penalty",
                "validity_penalty",
                "business_penalty",
            ],
        ),
        axis=1,
    )
    scored_df = pd.concat([df, scores], axis=1)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    staging_key = f"staging/{required_source_type or 'dataset'}_{timestamp}.parquet"
    staging_path = s3_uri(STORAGE_SETTINGS.silver_bucket, staging_key)
    scored_df.to_parquet(staging_path, index=False, storage_options=S3_OPTIONS)

    total_records = len(scored_df)
    pass_rate = (
        scored_df["quality_band"].isin(["EXCELLENT", "GOOD"]).sum() / total_records
    ) * 100
    poor_ratio = (
        scored_df["quality_band"].eq("POOR").sum() / total_records if total_records else 0
    )
    band_counts = {
        band: int(count) for band, count in scored_df["quality_band"].value_counts().items()
    }
    metrics = {
        "records": float(total_records),
        "quality_pass_rate": round(pass_rate, 2),
        "poor_share": round(poor_ratio, 4),
        "staging_path": staging_path,
        "quality_band_counts": band_counts,
        "avg_quality_score": round(float(scored_df["quality_score"].mean()), 4),
        "avg_deductions": round(float(scored_df["deductions"].mean()), 2),
        "schema_penalty_avg": round(float(scored_df["schema_penalty"].mean()), 2),
        "validity_penalty_avg": round(float(scored_df["validity_penalty"].mean()), 2),
        "business_penalty_avg": round(float(scored_df["business_penalty"].mean()), 2),
        "source_type": required_source_type or "mixed",
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
    }
    return staging_path, metrics


def quality_gate(metrics: Dict[str, float], poor_threshold: float = 0.2) -> bool:
    poor_share = metrics.get("poor_share", 0)
    return poor_share <= poor_threshold


def promote_to_silver(staging_path: str) -> str:
    if not staging_path:
        return ""
    df = pd.read_parquet(staging_path, storage_options=S3_OPTIONS)
    if df.empty:
        return ""

    df["start_time"] = pd.to_datetime(
        df["start_time"], utc=True, errors="coerce", format="ISO8601"
    )
    df["date"] = df["start_time"].dt.strftime("%Y-%m-%d")
    df["hour"] = df["start_time"].dt.strftime("%H")

    output_paths: List[str] = []
    for (date_value, hour_value), group in df.groupby(["date", "hour"]):
        key = f"date={date_value}/hour={hour_value}/silver_{date_value}_{hour_value}_{int(datetime.now().timestamp())}.parquet"
        out_path = s3_uri(STORAGE_SETTINGS.silver_bucket, key)
        group.drop(columns=["date", "hour"], errors="ignore").to_parquet(
            out_path, index=False, storage_options=S3_OPTIONS
        )
        output_paths.append(out_path)

    bucket, key = parse_s3_uri(staging_path)
    S3_CLIENT.delete_object(Bucket=bucket, Key=key)
    return output_paths[-1] if output_paths else ""


def update_gold_tables(silver_file: str) -> Dict[str, str]:
    if not silver_file:
        return {}
    df = pd.read_parquet(silver_file, storage_options=S3_OPTIONS)
    if df.empty:
        return {}

    df["start_time"] = pd.to_datetime(df["start_time"], utc=True)
    df["source_type"] = df.get("source_type", "unknown")

    def _upsert(path: str, new_df: pd.DataFrame, index_cols: List[str]) -> None:
        try:
            existing = pd.read_parquet(path, storage_options=S3_OPTIONS)
        except FileNotFoundError:
            existing = pd.DataFrame(columns=new_df.columns)
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = (
            combined.sort_values(index_cols)
            .drop_duplicates(subset=index_cols, keep="last")
            .reset_index(drop=True)
        )
        combined.to_parquet(path, index=False, storage_options=S3_OPTIONS)

    station_status = (
        df.groupby(["source_type", "start_station_id"])
        .agg(trips_started=("trip_id", "count"), avg_quality=("quality_score", "mean"))
        .reset_index()
    )
    hourly_usage = (
        df.assign(hour=df["start_time"].dt.floor("H"))
        .groupby(["source_type", "hour"])
        .agg(trips=("trip_id", "count"), avg_quality=("quality_score", "mean"))
        .reset_index()
    )
    quality_trend = (
        df.assign(date=df["start_time"].dt.date)
        .groupby(["source_type", "date"])
        .agg(
            mean_quality=("quality_score", "mean"),
            poor_share=("quality_band", lambda x: (x == "POOR").mean()),
        )
        .reset_index()
    )

    station_status_path = s3_uri(STORAGE_SETTINGS.gold_bucket, "station_status.parquet")
    hourly_usage_path = s3_uri(STORAGE_SETTINGS.gold_bucket, "hourly_usage.parquet")
    trend_path = s3_uri(STORAGE_SETTINGS.gold_bucket, "quality_trends.parquet")

    _upsert(station_status_path, station_status, ["source_type", "start_station_id"])
    _upsert(hourly_usage_path, hourly_usage, ["source_type", "hour"])
    _upsert(trend_path, quality_trend, ["source_type", "date"])

    return {
        "station_status": station_status_path,
        "hourly_usage": hourly_usage_path,
        "quality_trends": trend_path,
    }


def create_quality_report(metrics: Dict[str, float]) -> str:
    report_key = f"reports/quality_report_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    report_path = s3_uri(STORAGE_SETTINGS.gold_bucket, report_key)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "records": metrics.get("records", 0),
        "quality_pass_rate": metrics.get("quality_pass_rate", 0),
        "poor_share": metrics.get("poor_share", 0),
        "avg_quality_score": metrics.get("avg_quality_score"),
        "band_counts": metrics.get("quality_band_counts", {}),
        "schema_penalty_avg": metrics.get("schema_penalty_avg"),
        "validity_penalty_avg": metrics.get("validity_penalty_avg"),
        "business_penalty_avg": metrics.get("business_penalty_avg"),
        "source_type": metrics.get("source_type"),
        "status": "healthy" if metrics.get("poor_share", 0) <= 0.2 else "attention",
    }
    with S3_FS.open(report_path, "w") as handle:
        handle.write(json.dumps(report, indent=2))
    return report_path
