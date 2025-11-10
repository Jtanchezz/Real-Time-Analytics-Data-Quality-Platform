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
]
ALLOWED_FIELDS = REQUIRED_FIELDS + ["ingested_at"]
MAX_SPEED_KMH = 30
MIN_TRIP_DURATION = 60
MAX_TRIP_DURATION = 86_400  # 24 hours

STORAGE_SETTINGS = get_storage_settings()
S3_CLIENT = get_s3_client()
S3_FS = get_s3_fs()
S3_OPTIONS = STORAGE_SETTINGS.storage_options()
STATION_LOOKUP_PATH = REFERENCE_DIR / "stations_reference.csv"


def discover_new_data(window_minutes: int = 15) -> List[str]:
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
    paginator = S3_CLIENT.get_paginator("list_objects_v2")
    files: List[str] = []
    for page in paginator.paginate(Bucket=STORAGE_SETTINGS.bronze_bucket):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".parquet"):
                continue
            modified = obj["LastModified"].astimezone(timezone.utc)
            if modified >= cutoff:
                files.append(s3_uri(STORAGE_SETTINGS.bronze_bucket, obj["Key"]))
    return sorted(files)


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


def score_dataset(files: List[str]) -> Tuple[Optional[str], Dict[str, float]]:
    df = _load_frames(files)
    if df.empty:
        return None, {"records": 0, "quality_pass_rate": 0.0}

    stations = _load_station_lookup()

    def _score_row(row: pd.Series) -> Tuple[float, float, str]:
        deductions = 0
        missing_fields = [f for f in REQUIRED_FIELDS if pd.isna(row.get(f))]
        deductions += 10 * len(missing_fields)

        invalid_type_fields = []
        if row.get("rider_age") is not None and not pd.isna(row.get("rider_age")):
            try:
                int(row.get("rider_age"))
            except (TypeError, ValueError):
                invalid_type_fields.append("rider_age")
        numeric_fields = ["start_station_id", "end_station_id", "trip_duration"]
        for field in numeric_fields:
            value = row.get(field)
            if value is not None and pd.isna(value):
                invalid_type_fields.append(field)
        deductions += 5 * len(invalid_type_fields)

        unexpected_fields = [field for field in row.index if field not in ALLOWED_FIELDS]
        deductions += 2 * len(unexpected_fields)

        try:
            start = pd.to_datetime(row.get("start_time"), utc=True)
            end = pd.to_datetime(row.get("end_time"), utc=True)
        except Exception:
            start = end = None
            deductions += 5
        else:
            if start >= end:
                deductions += 25
            duration_delta = (end - start).total_seconds()
            try:
                provided_duration = float(row.get("trip_duration"))
            except (TypeError, ValueError):
                provided_duration = duration_delta
            if abs(duration_delta - provided_duration) > 60:
                deductions += 15

        age = row.get("rider_age")
        if age is not None and not pd.isna(age):
            if not (16 <= float(age) <= 100):
                deductions += 20

        for field in ("start_station_id", "end_station_id"):
            station_value = row.get(field)
            if station_value is None or pd.isna(station_value):
                deductions += 10
                continue
            if stations and int(station_value) not in stations:
                deductions += 10

        duration_value = row.get("trip_duration")
        if duration_value is not None and not pd.isna(duration_value):
            duration_value = int(duration_value)
            if duration_value < MIN_TRIP_DURATION or duration_value > MAX_TRIP_DURATION:
                deductions += 15
            if start and end and row.get("start_station_id") == row.get("end_station_id"):
                if duration_value > 3600:
                    deductions += 5
        speed = _estimate_speed_kmh(row, stations)
        if speed and speed > MAX_SPEED_KMH:
            deductions += 10

        score = max(BASE_SCORE - deductions, 0)
        band = _score_band(score)
        return score, deductions, band

    scores = df.apply(
        lambda r: pd.Series(
            _score_row(r), index=["quality_score", "deductions", "quality_band"]
        ),
        axis=1,
    )
    scored_df = pd.concat([df, scores], axis=1)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    staging_key = f"staging/realtime_quality_{timestamp}.parquet"
    staging_path = s3_uri(STORAGE_SETTINGS.silver_bucket, staging_key)
    scored_df.to_parquet(staging_path, index=False, storage_options=S3_OPTIONS)

    pass_rate = (
        scored_df["quality_band"].isin(["EXCELLENT", "GOOD"]).sum() / len(scored_df)
    ) * 100
    metrics = {
        "records": float(len(scored_df)),
        "quality_pass_rate": round(pass_rate, 2),
        "poor_share": round(
            scored_df["quality_band"].isin(["POOR"]).sum() / len(scored_df)
            if len(scored_df)
            else 0,
            2,
        ),
        "staging_path": staging_path,
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

    df["start_time"] = pd.to_datetime(df["start_time"], utc=True)
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
    station_status = (
        df.groupby("start_station_id")
        .agg(trips_started=("trip_id", "count"), avg_quality=("quality_score", "mean"))
        .reset_index()
    )
    hourly_usage = (
        df.assign(hour=df["start_time"].dt.floor("H"))
        .groupby("hour")
        .agg(trips=("trip_id", "count"), avg_quality=("quality_score", "mean"))
        .reset_index()
    )
    quality_trend = (
        df.assign(date=df["start_time"].dt.date)
        .groupby("date")
        .agg(
            mean_quality=("quality_score", "mean"),
            poor_share=("quality_band", lambda x: (x == "POOR").mean()),
        )
        .reset_index()
    )

    station_status_path = s3_uri(STORAGE_SETTINGS.gold_bucket, "station_status.parquet")
    hourly_usage_path = s3_uri(STORAGE_SETTINGS.gold_bucket, "hourly_usage.parquet")
    trend_path = s3_uri(STORAGE_SETTINGS.gold_bucket, "quality_trends.parquet")

    station_status.to_parquet(station_status_path, index=False, storage_options=S3_OPTIONS)
    hourly_usage.to_parquet(hourly_usage_path, index=False, storage_options=S3_OPTIONS)
    quality_trend.to_parquet(trend_path, index=False, storage_options=S3_OPTIONS)

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
        "status": "healthy" if metrics.get("poor_share", 0) <= 0.2 else "attention",
    }
    with S3_FS.open(report_path, "w") as handle:
        handle.write(json.dumps(report, indent=2))
    return report_path
