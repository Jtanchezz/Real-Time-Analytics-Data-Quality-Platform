from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import httpx
import pandas as pd
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from workflows.paths import get_s3_client, get_s3_fs, get_storage_settings, s3_uri

st.set_page_config(
    page_title="NYC Bikes – Data Platform",
    page_icon="🚲",
    layout="wide",
)

SETTINGS = get_storage_settings()
S3_FS = get_s3_fs()
S3_CLIENT = get_s3_client()
S3_OPTIONS = SETTINGS.storage_options()
API_BASE_URL = os.getenv("APP_METRICS_API_URL", "http://localhost:8000").rstrip("/")
API_METRICS_ENDPOINT = f"{API_BASE_URL}/api/v1/metrics"


def _count_objects(bucket: str) -> int:
    paginator = S3_CLIENT.get_paginator("list_objects_v2")
    total = 0
    try:
        for page in paginator.paginate(Bucket=bucket):
            total += len(page.get("Contents", []))
    except Exception:
        return 0
    return total


def _fallback_storage_counts() -> Dict[str, int]:
    return {
        "bronze_files": _count_objects(SETTINGS.bronze_bucket),
        "silver_files": _count_objects(SETTINGS.silver_bucket),
        "gold_files": _count_objects(SETTINGS.gold_bucket),
    }


def _fetch_live_api_metrics() -> Dict[str, object]:
    try:
        response = httpx.get(API_METRICS_ENDPOINT, timeout=5.0)
        response.raise_for_status()
        payload = response.json()
        payload["reachable"] = True
    except Exception:
        payload = {
            "events_per_minute": None,
            "errors_per_minute": None,
            "avg_latency_ms": None,
            "reachable": False,
        }
    payload["endpoint"] = API_METRICS_ENDPOINT
    return payload


@st.cache_data(ttl=60)
def _load_pipeline_metrics() -> Dict[str, object]:
    payload: Dict[str, object] = {}
    path = s3_uri(SETTINGS.gold_bucket, "dashboards/pipeline_metrics.json")
    try:
        with S3_FS.open(path, "r") as handle:
            payload = json.load(handle)
    except FileNotFoundError:
        payload = {}
    except json.JSONDecodeError:
        payload = {}

    api_metrics = payload.get("api")
    if not isinstance(api_metrics, dict) or not api_metrics:
        payload["api"] = _fetch_live_api_metrics()
    else:
        api_metrics.setdefault("endpoint", API_METRICS_ENDPOINT)
        api_metrics.setdefault("reachable", False)
        payload["api"] = api_metrics

    storage_metrics = payload.get("storage")
    required_keys = {"bronze_files", "silver_files", "gold_files"}
    if not isinstance(storage_metrics, dict) or not required_keys.issubset(storage_metrics):
        payload["storage"] = _fallback_storage_counts()
    else:
        payload["storage"] = storage_metrics

    payload.setdefault("generated_at", datetime.now(timezone.utc).isoformat())
    return payload


@st.cache_data(ttl=300)
def _load_quality_trends() -> pd.DataFrame:
    path = s3_uri(SETTINGS.gold_bucket, "quality_trends.parquet")
    try:
        df = pd.read_parquet(path, storage_options=S3_OPTIONS)
    except FileNotFoundError:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date")


@st.cache_data(ttl=300)
def _load_hourly_usage() -> pd.DataFrame:
    path = s3_uri(SETTINGS.gold_bucket, "hourly_usage.parquet")
    try:
        df = pd.read_parquet(path, storage_options=S3_OPTIONS)
    except FileNotFoundError:
        return pd.DataFrame()
    df["hour"] = pd.to_datetime(df["hour"])
    return df.sort_values("hour")


@st.cache_data(ttl=120)
def _load_station_status() -> pd.DataFrame:
    path = s3_uri(SETTINGS.gold_bucket, "station_status.parquet")
    try:
        return pd.read_parquet(path, storage_options=S3_OPTIONS)
    except FileNotFoundError:
        return pd.DataFrame()


@st.cache_data(ttl=120)
def _load_flow_status() -> pd.DataFrame:
    prefix = s3_uri(SETTINGS.gold_bucket, "dashboards/flow_status")
    matches: List[str] = []
    try:
        matches = S3_FS.glob(f"{prefix}/*.json")
    except FileNotFoundError:
        pass
    records: List[Dict[str, object]] = []
    for path in matches:
        try:
            with S3_FS.open(path, "r") as handle:
                payload = json.load(handle)
        except FileNotFoundError:
            continue
        except OSError:
            continue
        payload["path"] = path
        records.append(payload)
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    df["updated_at"] = pd.to_datetime(df["updated_at"])
    return df.sort_values("updated_at", ascending=False)


@st.cache_data(ttl=120)
def _load_quality_reports(limit: int = 10) -> pd.DataFrame:
    prefix = s3_uri(SETTINGS.gold_bucket, "reports")
    matches: List[str] = []
    try:
        matches = sorted(S3_FS.glob(f"{prefix}/quality_report_*.json"))
    except FileNotFoundError:
        pass
    records: List[Dict[str, object]] = []
    for path in matches[-limit:]:
        try:
            with S3_FS.open(path, "r") as handle:
                payload = json.load(handle)
        except FileNotFoundError:
            continue
        except OSError:
            continue
        payload["report_path"] = path
        records.append(payload)
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    df["generated_at"] = pd.to_datetime(df["generated_at"])
    return df.sort_values("generated_at", ascending=False)


def _metric(value: object, label: str, delta: float | None = None) -> None:
    st.metric(label, value if value is not None else "—", delta)


def render_api_ingest_section(metrics: Dict[str, object]) -> None:
    st.subheader("API Ingesta y Latencia")
    col1, col2, col3 = st.columns(3)
    api = metrics.get("api", {})
    with col1:
        _metric(api.get("events_per_minute"), "Eventos/min")
    with col2:
        _metric(api.get("errors_per_minute"), "Errores/min")
    with col3:
        _metric(api.get("avg_latency_ms"), "Latencia promedio (ms)")
    st.caption(f"Endpoint: {api.get('endpoint', 'N/D')} | Reachable: {api.get('reachable')}")
    if not api.get("reachable"):
        st.warning("API de métricas no disponible. Levanta FastAPI o revisa APP_METRICS_API_URL.")


def render_quality_section(trends: pd.DataFrame, reports: pd.DataFrame) -> None:
    st.subheader("Calidad de Datos")
    if not reports.empty:
        latest = reports.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Eventos evaluados", f"{int(latest['records']):,}")
        c2.metric("Pass rate %", latest.get("quality_pass_rate"))
        c3.metric("Poor share", latest.get("poor_share"))
        c4.metric("Avg Quality Score", round(latest.get("avg_quality_score", 0) * 100, 2))
        st.caption(f"Último reporte: {latest['generated_at'].strftime('%Y-%m-%d %H:%M:%S UTC')}")

        band_counts = latest.get("band_counts") or {}
        total_records = float(latest.get("records") or 0)
        if band_counts and total_records > 0:
            dist_rows = [
                {"band": band, "percent": (count / total_records) * 100.0}
                for band, count in band_counts.items()
            ]
            dist_df = pd.DataFrame(dist_rows).sort_values("band")
            st.subheader("Distribución de calidad (%)")
            st.bar_chart(dist_df.set_index("band")["percent"])
    else:
        st.info("Aún no hay reportes de calidad generados.")

    if not trends.empty:
        if "source_type" in trends.columns:
            pivot = (
                trends.pivot_table(
                    index="date",
                    columns="source_type",
                    values="mean_quality",
                    aggfunc="mean",
                )
                .sort_index()
            )
            st.subheader("Tendencia de Quality Score (Histórico vs Realtime)")
            st.line_chart(pivot, height=260)
        else:
            trends_display = trends.set_index("date")[["mean_quality", "poor_share"]]
            st.line_chart(trends_display, height=260)
    else:
        st.warning("No se encontraron datos en gold/quality_trends.parquet")

    if not reports.empty:
        with st.expander("Historial de reportes"):
            display_cols = ["generated_at", "quality_pass_rate", "poor_share", "band_counts", "report_path"]
            st.dataframe(reports[display_cols])


def render_usage_section(hourly: pd.DataFrame, station: pd.DataFrame) -> None:
    st.subheader("Uso y Demanda")
    if not hourly.empty:
        available = max(1, min(48, len(hourly)))
        slider_step = 1 if available < 6 else 6
        default_window = min(24, available)

        if available == 1:
            st.info("Sólo hay una hora disponible para mostrar.")
            last_hours = 1
        else:
            last_hours = st.slider(
                "Ventana (últimas horas)",
                min_value=1,
                max_value=available,
                value=default_window,
                step=slider_step,
            )

        hourly_tail = hourly.tail(last_hours)
        if hourly_tail.empty:
            st.warning("No hay suficientes datos para graficar la serie horaria todavía.")
        else:
            st.area_chart(hourly_tail.set_index("hour")[["trips", "avg_quality"]])
    else:
        st.warning("No hay datos en gold/hourly_usage.parquet")


    if not station.empty:
        st.dataframe(
            station.sort_values("trips_started", ascending=False).head(20),
            hide_index=True,
        )
    else:
        st.info("Sin datos de estaciones en gold/station_status.parquet")


def render_flow_health_section(metrics: Dict[str, object], statuses: pd.DataFrame) -> None:
    st.subheader("Salud del Pipeline")
    storage = metrics.get("storage", {})
    st.write(
        f"Archivos en Bronze: {storage.get('bronze_files', 0)}, "
        f"Silver: {storage.get('silver_files', 0)}, Gold: {storage.get('gold_files', 0)}"
    )
    if statuses.empty:
        st.info("No hay estados registrados en gold/dashboards/flow_status/")
    else:
        st.dataframe(statuses[["flow", "status", "updated_at", "path"]], hide_index=True)


def main() -> None:
    st.title("NYC Bikes – Data Platform Dashboards")
    metrics = _load_pipeline_metrics()
    trends = _load_quality_trends()
    usage = _load_hourly_usage()
    stations = _load_station_status()
    flows = _load_flow_status()
    reports = _load_quality_reports()

    ingestion_tab, quality_tab, health_tab = st.tabs(
        ["Ingestion Dashboard", "Data Quality Dashboard", "Pipeline Health Dashboard"]
    )

    with ingestion_tab:
        render_api_ingest_section(metrics)
        st.markdown("---")
        render_usage_section(usage, stations)

    with quality_tab:
        render_quality_section(trends, reports)

    with health_tab:
        render_flow_health_section(metrics, flows)
        st.markdown(
            f"Última actualización del snapshot: "
            f"{metrics.get('generated_at', 'N/D')}"
        )


if __name__ == "__main__":
    main()
