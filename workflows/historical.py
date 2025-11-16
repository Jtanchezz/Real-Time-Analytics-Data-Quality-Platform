from __future__ import annotations

import json
import os
import re
import tempfile
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Dict, List, Optional

import httpx
import pandas as pd

from .paths import (
    get_s3_client,
    get_s3_fs,
    get_storage_settings,
    parse_s3_uri,
    s3_uri,
)
from .quality import (
    REQUIRED_FIELDS,
    promote_to_silver,
    score_dataset,
    update_gold_tables,
)

STORAGE_SETTINGS = get_storage_settings()
S3_CLIENT = get_s3_client()
S3_FS = get_s3_fs()
S3_OPTIONS = STORAGE_SETTINGS.storage_options()

PENDING_PREFIX = "pending/"
PROCESSED_PREFIX = "processed/"
MANIFEST_KEY = "control/archive_manifest.json"
MANIFEST_URI = s3_uri(STORAGE_SETTINGS.historical_bucket, MANIFEST_KEY)
NYC_TRIPDATA_INDEX = "https://s3.amazonaws.com/tripdata/index.html"
ARCHIVE_PATTERN = re.compile(r'href="([^"]+\.zip)"', re.IGNORECASE)
ARCHIVE_NAME_PATTERN = re.compile(r"(20\d{2})[-_]?(\d{2})")
IGNORED_FILES = {".ds_store", "thumbs.db"}
HISTORICAL_YEAR_FILTER = {
    value.strip()
    for value in os.getenv("APP_HISTORICAL_YEARS", "").split(",")
    if value.strip()
}


def _load_archive_manifest() -> Dict[str, Dict[str, str]]:
    try:
        with S3_FS.open(MANIFEST_URI, "r") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        data = {}
    except Exception:
        data = {}
    archives = data.get("archives", {})
    return {"archives": archives}


def _persist_archive_manifest(manifest: Dict[str, Dict[str, str]]) -> None:
    payload = {"archives": manifest.get("archives", {})}
    with S3_FS.open(MANIFEST_URI, "w") as handle:
        json.dump(payload, handle)


def _archive_slug(name: str) -> str:
    stem = PurePosixPath(name).stem
    if stem.endswith(".csv"):
        stem = PurePosixPath(stem).stem
    slug = re.sub(r"[^0-9a-zA-Z]+", "-", stem.lower()).strip("-")
    return slug or stem.lower()


def _list_remote_archives() -> List[Dict[str, str]]:
    response = httpx.get(NYC_TRIPDATA_INDEX, timeout=60.0)
    response.raise_for_status()
    archives: List[Dict[str, str]] = []
    for href in ARCHIVE_PATTERN.findall(response.text):
        name = PurePosixPath(href).name or href.split("/")[-1]
        if not name or not name.startswith("20") or name.upper().startswith("JC"):
            continue
        match = ARCHIVE_NAME_PATTERN.search(name)
        year = match.group(1) if match else None
        month = match.group(2) if match else None
        slug = _archive_slug(name)
        url = href if href.startswith("http") else f"https://s3.amazonaws.com/tripdata/{name}"
        archives.append(
            {
                "name": name,
                "url": url,
                "year": year,
                "month": month,
                "slug": slug,
            }
        )
    archives.sort(key=lambda item: item["name"])
    if HISTORICAL_YEAR_FILTER:
        archives = [arc for arc in archives if arc["year"] in HISTORICAL_YEAR_FILTER]
    return archives


def _pending_batches() -> List[str]:
    paginator = S3_CLIENT.get_paginator("list_objects_v2")
    files: List[str] = []
    for page in paginator.paginate(
        Bucket=STORAGE_SETTINGS.historical_bucket, Prefix=PENDING_PREFIX
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            files.append(s3_uri(STORAGE_SETTINGS.historical_bucket, key))
    return sorted(files)


def _move_to_processed(uri: str) -> None:
    bucket, key = parse_s3_uri(uri)
    source = {"Bucket": bucket, "Key": key}
    processed_key = key.replace(PENDING_PREFIX, PROCESSED_PREFIX, 1)
    S3_CLIENT.copy_object(Bucket=bucket, CopySource=source, Key=processed_key)
    S3_CLIENT.delete_object(Bucket=bucket, Key=key)


def _extract_csv_files(zip_path: Path, work_dir: Path) -> List[Path]:
    csv_paths: List[Path] = []

    def _walk(archive_path: Path, target_dir: Path) -> None:
        with zipfile.ZipFile(archive_path) as handle:
            for member in handle.infolist():
                if member.is_dir():
                    continue
                member_name = Path(member.filename)
                if any(part.startswith("__MACOSX") for part in member_name.parts):
                    continue
                if member_name.name.startswith("._"):
                    continue
                suffix = member_name.suffix.lower()
                safe_name = member_name.name
                target_path = target_dir / safe_name
                target_path.parent.mkdir(parents=True, exist_ok=True)
                with target_path.open("wb") as outfile:
                    outfile.write(handle.read(member))
                if suffix == ".zip":
                    nested_dir = target_path.parent / target_path.stem
                    nested_dir.mkdir(parents=True, exist_ok=True)
                    _walk(target_path, nested_dir)
                elif suffix == ".csv":
                    if target_path.name.lower() in IGNORED_FILES:
                        continue
                    csv_paths.append(target_path)

    _walk(zip_path, work_dir)
    return csv_paths


def _column(df: pd.DataFrame, candidates: List[str]) -> pd.Series:
    for candidate in candidates:
        if candidate in df.columns:
            return df[candidate]
    return pd.Series([None] * len(df))


def _normalize_csv(csv_path: Path, archive_year: Optional[str]) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    if df.empty:
        return df
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    start_series = pd.to_datetime(
        _column(df, ["started_at", "starttime", "start_time"]), errors="coerce", utc=True
    )
    end_series = pd.to_datetime(
        _column(df, ["ended_at", "stoptime", "stop_time"]), errors="coerce", utc=True
    )
    duration_series = pd.to_numeric(
        _column(df, ["tripduration", "duration", "ride_length", "ride_length_seconds"]),
        errors="coerce",
    )
    if duration_series.isna().all() and start_series.notna().any() and end_series.notna().any():
        duration_series = (end_series - start_series).dt.total_seconds()

    trip_id = _column(df, ["trip_id", "ride_id", "id"]).astype(str).replace("nan", pd.NA)
    if trip_id.isna().any():
        fallback_ids = pd.Series(
            [f"{csv_path.stem}_{idx}" for idx in range(len(trip_id))],
            index=trip_id.index,
        )
        trip_id = trip_id.where(trip_id.notna(), fallback_ids)

    bike_id = pd.to_numeric(_column(df, ["bike_id", "bikeid"]), errors="coerce")
    if bike_id.isna().all():
        hashed = pd.util.hash_pandas_object(trip_id, index=False).abs()
        bike_id = (hashed % 1_000_000_000).astype("int64")

    start_station = pd.to_numeric(
        _column(df, ["start_station_id", "start_station"]), errors="coerce"
    )
    end_station = pd.to_numeric(
        _column(df, ["end_station_id", "end_station"]), errors="coerce"
    )

    rider_age = pd.to_numeric(_column(df, ["rider_age"]), errors="coerce")
    if rider_age.isna().all():
        birth_year = pd.to_numeric(
            _column(df, ["birth_year", "member_birth_year"]), errors="coerce"
        )
        event_year = (
            start_series.dt.year.fillna(int(archive_year)) if archive_year else start_series.dt.year
        )
        rider_age = event_year - birth_year

    member_casual_series = _column(df, ["member_casual", "usertype", "user_type"]).astype(str)

    bike_type_series = _column(df, ["bike_type", "rideable_type"]).astype(str)
    bike_type_series = bike_type_series.replace("nan", "unknown").fillna("unknown")

    normalized = pd.DataFrame(
        {
            "trip_id": trip_id.astype(str),
            "bike_id": bike_id,
            "start_time": start_series,
            "end_time": end_series,
            "start_station_id": start_station,
            "end_station_id": end_station,
            "rider_age": rider_age,
            "trip_duration": duration_series,
            "bike_type": bike_type_series,
            "member_casual": member_casual_series,
        }
    )

    def _normalize_membership(value: str) -> str:
        lowered = (value or "").strip().lower()
        if lowered in {"nan", "none", "null"}:
            return "unknown"
        if lowered in {"subscriber", "member"}:
            return "member"
        if lowered in {"customer", "casual"}:
            return "casual"
        return lowered or "unknown"

    normalized["member_casual"] = normalized["member_casual"].apply(_normalize_membership)
    normalized["trip_duration"] = pd.to_numeric(normalized["trip_duration"], errors="coerce")
    normalized = normalized.drop_duplicates(subset=["trip_id"], keep="last")
    return normalized


def _stage_archive(
    archive: Dict[str, str], manifest: Dict[str, Dict[str, Dict[str, str]]]
) -> Optional[str]:
    with tempfile.TemporaryDirectory() as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        archive_path = temp_dir / archive["name"]
        with httpx.stream("GET", archive["url"], timeout=None) as response:
            response.raise_for_status()
            with archive_path.open("wb") as handle:
                for chunk in response.iter_bytes():
                    handle.write(chunk)
        csv_files = _extract_csv_files(archive_path, temp_dir / archive["slug"])
        frames = [
            frame
            for frame in (
                _normalize_csv(path, archive.get("year")) for path in csv_files
            )
            if not frame.empty
        ]
        if not frames:
            return None
        dataset = pd.concat(frames, ignore_index=True)
        dataset["source_type"] = "historical"
        dataset["ingested_at"] = datetime.now(timezone.utc).isoformat()
        dataset = dataset[[*REQUIRED_FIELDS, "ingested_at", "source_type"]]
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        staged_key = (
            f"{PENDING_PREFIX}archive={archive['slug']}/historical_{archive['slug']}_{timestamp}.parquet"
        )
        staged_uri = s3_uri(STORAGE_SETTINGS.historical_bucket, staged_key)
        dataset.to_parquet(staged_uri, index=False, storage_options=S3_OPTIONS)
        manifest["archives"][archive["name"]] = {
            "status": "pending",
            "slug": archive["slug"],
            "staged_key": staged_key,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        return staged_uri


def validate_historical_batches() -> Dict[str, List[str]]:
    manifest = _load_archive_manifest()
    pending_existing = _pending_batches()

    remote_archives = _list_remote_archives()
    already_processed = [
        name for name, meta in manifest["archives"].items() if meta.get("status") == "processed"
    ]
    staged_files: List[str] = []
    invalid_archives: List[str] = []
    staged_archives: List[str] = []

    for archive in remote_archives:
        entry = manifest["archives"].get(archive["name"])
        if entry and entry.get("status") in {"pending", "processed"}:
            continue
        try:
            staged_uri = _stage_archive(archive, manifest)
        except Exception:
            invalid_archives.append(archive["name"])
            continue
        if staged_uri:
            staged_files.append(staged_uri)
            staged_archives.append(archive["name"])

    if staged_files:
        _persist_archive_manifest(manifest)

    valid_files = sorted(set(pending_existing + staged_files))
    return {
        "valid_files": valid_files,
        "archives": staged_archives,
        "invalid_archives": invalid_archives,
        "already_processed": already_processed,
    }


def _archive_name_from_stage(stage_key: str, manifest: Dict[str, Dict[str, Dict[str, str]]]) -> Optional[str]:
    for name, meta in manifest["archives"].items():
        if meta.get("staged_key") == stage_key:
            return name
    return None


def load_historical_to_bronze(valid_files: List[str]) -> List[str]:
    bronze_paths: List[str] = []
    manifest = _load_archive_manifest()
    for uri in valid_files:
        df = pd.read_parquet(uri, storage_options=S3_OPTIONS)
        if df.empty:
            _move_to_processed(uri)
            continue
        df["start_time"] = pd.to_datetime(df["start_time"], utc=True, errors="coerce")
        df = df.dropna(subset=["start_time"])
        df["end_time"] = pd.to_datetime(df["end_time"], utc=True, errors="coerce")
        df["source_type"] = "historical"
        default_ingested = datetime.now(timezone.utc).isoformat()
        existing_ingested = df.get("ingested_at")
        if existing_ingested is None:
            df["ingested_at"] = default_ingested
        else:
            df["ingested_at"] = existing_ingested.fillna(default_ingested)
        df["date"] = df["start_time"].dt.strftime("date=%Y-%m-%d")
        df["hour"] = df["start_time"].dt.strftime("hour=%H")
        _, stage_key = parse_s3_uri(uri)
        archive_name = _archive_name_from_stage(stage_key, manifest)
        archive_slug = manifest["archives"].get(archive_name, {}).get("slug") if archive_name else "historical"
        for (date_value, hour_value), group in df.groupby(["date", "hour"]):
            bronze_key = (
                f"historical/{date_value}/{hour_value}/"
                f"{archive_slug}_{uuid.uuid4().hex}.parquet"
            )
            bronze_uri = s3_uri(STORAGE_SETTINGS.bronze_bucket, bronze_key)
            group.drop(columns=["date", "hour"], errors="ignore").to_parquet(
                bronze_uri, index=False, storage_options=S3_OPTIONS
            )
            bronze_paths.append(bronze_uri)
        _move_to_processed(uri)
        if archive_name and archive_name in manifest["archives"]:
            manifest["archives"][archive_name]["status"] = "processed"
            manifest["archives"][archive_name]["staged_key"] = None
            manifest["archives"][archive_name]["updated_at"] = datetime.now(timezone.utc).isoformat()
    _persist_archive_manifest(manifest)
    return bronze_paths


def process_historical_to_silver(bronze_files: List[str]) -> Dict[str, object]:
    if not bronze_files:
        return {"silver_path": "", "records": 0, "quality_pass_rate": 0.0, "poor_share": 0.0}
    staging_path, metrics = score_dataset(bronze_files, required_source_type="historical")
    if not staging_path:
        return {"silver_path": "", **metrics}
    silver_path = promote_to_silver(staging_path)
    gold_outputs = update_gold_tables(silver_path) if silver_path else {}
    return {
        "silver_path": silver_path,
        **metrics,
        "gold_outputs": gold_outputs,
    }
