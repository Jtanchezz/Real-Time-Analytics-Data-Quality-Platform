from __future__ import annotations

import argparse
import io
import logging
import re
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, List

import boto3
import pandas as pd
from botocore import UNSIGNED
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from workflows.paths import get_storage_settings, s3_uri  # noqa: E402

PUBLIC_BUCKET = "tripdata"
PUBLIC_S3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

STORAGE_SETTINGS = get_storage_settings()
S3_OPTIONS = STORAGE_SETTINGS.storage_options()

REQUIRED_COLUMNS = [
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
OUTPUT_COLUMNS = REQUIRED_COLUMNS + ["ingested_at"]

COLUMN_ALIASES = {
    "ride_id": "trip_id",
    "tripid": "trip_id",
    "tripduration": "trip_duration",
    "starttime": "start_time",
    "started_at": "start_time",
    "stoptime": "end_time",
    "ended_at": "end_time",
    "bikeid": "bike_id",
    "bike_id": "bike_id",
    "usertype": "member_casual",
    "rideable_type": "bike_type",
    "member_birth_year": "birth_year",
    "birthyear": "birth_year",
}

MEMBER_MAP = {
    "subscriber": "member",
    "member": "member",
    "annual": "member",
    "customer": "casual",
    "casual": "casual",
    "day_pass": "casual",
}


def _normalize_column_name(name: str) -> str:
    clean = name.strip().lower()
    clean = re.sub(r"[^0-9a-z]+", "_", clean)
    return clean.strip("_")


def _clean_slug(value: str) -> str:
    slug = re.sub(r"[^0-9a-zA-Z]+", "_", value)
    return slug.strip("_").lower()


def _string_series(series: pd.Series) -> pd.Series:
    if series is None:
        return pd.Series(pd.NA, dtype="string[pyarrow]")
    converted = series.astype("string")
    converted = converted.str.strip()
    return converted.replace({"": pd.NA, "none": pd.NA, "nan": pd.NA})


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = df.rename(columns=_normalize_column_name)
    df = df.rename(columns={k: v for k, v in COLUMN_ALIASES.items() if k in df.columns})

    if "start_time" not in df.columns:
        df["start_time"] = pd.NaT
    if "end_time" not in df.columns:
        df["end_time"] = pd.NaT

    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce", utc=True)
    df["end_time"] = pd.to_datetime(df["end_time"], errors="coerce", utc=True)

    if "trip_id" not in df.columns:
        df["trip_id"] = pd.NA
    df["trip_id"] = _string_series(df["trip_id"])

    bike_series = None
    if "bike_id" in df.columns:
        bike_series = pd.to_numeric(df["bike_id"], errors="coerce")
    if bike_series is None:
        bike_series = pd.Series(pd.NA, index=df.index, dtype="Float64")
    df["bike_id"] = bike_series
    missing_bike = df["bike_id"].isna()
    if missing_bike.any():
        generated = pd.factorize(df.loc[missing_bike, "trip_id"])[0] + 1
        df.loc[missing_bike, "bike_id"] = generated
    df["bike_id"] = pd.to_numeric(df["bike_id"], errors="coerce").astype("Int64")

    for field in ("start_station_id", "end_station_id"):
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors="coerce")
        else:
            df[field] = pd.NA
        df[field] = df[field].astype("Float64")

    if "trip_duration" in df.columns:
        df["trip_duration"] = pd.to_numeric(df["trip_duration"], errors="coerce")
    else:
        df["trip_duration"] = pd.NA
    needs_duration = df["trip_duration"].isna()
    if needs_duration.any():
        duration = (df["end_time"] - df["start_time"]).dt.total_seconds()
        df.loc[needs_duration, "trip_duration"] = duration.loc[needs_duration]
    df["trip_duration"] = pd.to_numeric(df["trip_duration"], errors="coerce").round()
    df["trip_duration"] = df["trip_duration"].astype("Int64")

    if "birth_year" in df.columns:
        birth_year = pd.to_numeric(df["birth_year"], errors="coerce")
        df["rider_age"] = df["start_time"].dt.year - birth_year
    elif "rider_age" not in df.columns:
        df["rider_age"] = pd.NA
    df["rider_age"] = pd.to_numeric(df["rider_age"], errors="coerce")
    df["rider_age"] = df["rider_age"].astype("Int64")

    if "bike_type" in df.columns:
        df["bike_type"] = _string_series(df["bike_type"]).str.lower()
    else:
        df["bike_type"] = pd.Series(pd.NA, index=df.index, dtype="string[pyarrow]")
    df["bike_type"] = df["bike_type"].fillna("unknown")

    if "member_casual" in df.columns:
        df["member_casual"] = _string_series(df["member_casual"]).str.lower()
    else:
        df["member_casual"] = pd.Series(pd.NA, index=df.index, dtype="string[pyarrow]")
    df["member_casual"] = df["member_casual"].map(MEMBER_MAP).fillna(
        df["member_casual"]
    )
    df["member_casual"] = df["member_casual"].fillna("unknown")

    df["ingested_at"] = datetime.now(timezone.utc)

    for column in OUTPUT_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA

    normalized = df[OUTPUT_COLUMNS].copy()
    normalized = normalized.dropna(subset=["start_time", "end_time"], how="any")
    return normalized


def _iter_remote_keys(prefix: str = "") -> Iterator[str]:
    paginator = PUBLIC_S3.get_paginator("list_objects_v2")
    kwargs = {"Bucket": PUBLIC_BUCKET}
    if prefix:
        kwargs["Prefix"] = prefix
    for page in paginator.paginate(**kwargs):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            name = key.rsplit("/", 1)[-1]
            if not name.lower().endswith(".zip"):
                continue
            if not re.match(r"^\d", name):
                continue
            if name.upper().startswith("JC"):
                continue
            yield key


def _read_zip_object(key: str) -> bytes:
    logging.info("Descargando %s", key)
    obj = PUBLIC_S3.get_object(Bucket=PUBLIC_BUCKET, Key=key)
    return obj["Body"].read()


def _extract_csv_frames(zip_bytes: bytes, slug: str) -> List[pd.DataFrame]:
    frames: List[pd.DataFrame] = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        for info in archive.infolist():
            if info.is_dir():
                continue
            name = info.filename
            lowered = name.lower()
            if "__macosx" in lowered or Path(name).name.startswith("._"):
                logging.info("  Saltando archivo auxiliar %s", name)
                continue
            with archive.open(info) as file_handle:
                payload = file_handle.read()
            if lowered.endswith(".zip"):
                nested_slug = _clean_slug(f"{slug}_{Path(name).stem}")
                frames.extend(_extract_csv_frames(payload, nested_slug))
                continue
            if not lowered.endswith(".csv"):
                continue
            inner_slug = _clean_slug(f"{slug}_{Path(name).stem}")
            logging.info("  Procesando %s", name)
            frame = pd.read_csv(io.BytesIO(payload))
            normalized = _normalize_dataframe(frame)
            if not normalized.empty:
                frames.append(normalized)
                logging.info(
                    "  %s filas después de normalizar %s", len(normalized), name
                )
            else:
                logging.info("  %s no contenía registros válidos", name)
    return frames


def _combine_and_write(frames: Iterable[pd.DataFrame], key: str) -> str | None:
    joined = pd.concat(list(frames), ignore_index=True) if frames else pd.DataFrame()
    if joined.empty:
        logging.warning("No se generaron registros para %s", key)
        return None

    start_times = joined["start_time"].dropna()
    if not start_times.empty:
        first_start = start_times.min()
        year = first_start.year
        month = first_start.month
        prefix = f"pending/year={year}/month={month:02d}"
    else:
        prefix = "pending/year=unknown/month=unknown"

    slug = _clean_slug(Path(key).stem)
    output_key = f"{prefix}/{slug}.parquet"
    output_uri = s3_uri(STORAGE_SETTINGS.historical_bucket, output_key)
    logging.info("Escribiendo %s filas en %s", len(joined), output_uri)
    joined.to_parquet(output_uri, index=False, storage_options=S3_OPTIONS)
    return output_uri


def download_historical(prefix: str = "", limit: int | None = None) -> List[str]:
    written: List[str] = []
    for idx, key in enumerate(_iter_remote_keys(prefix), start=1):
        if limit is not None and idx > limit:
            break
        zip_bytes = _read_zip_object(key)
        frames = _extract_csv_frames(zip_bytes, _clean_slug(Path(key).stem))
        uri = _combine_and_write(frames, key)
        if uri:
            written.append(uri)
    return written


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Descarga archivos históricos de CitiBike desde s3://tripdata "
            "y los guarda como Parquet en historical/pending/."
        )
    )
    parser.add_argument(
        "--prefix",
        default="",
        help="Prefijo opcional para filtrar los ZIP remotos (ej. 2021).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cantidad máxima de ZIPs a procesar (útil para pruebas).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nivel de logging a mostrar.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(message)s")
    written = download_historical(prefix=args.prefix, limit=args.limit)
    logging.info("Listo. Se escribieron %s archivos.", len(written))


if __name__ == "__main__":
    main()
