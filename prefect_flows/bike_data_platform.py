from __future__ import annotations

from typing import Dict, List

from prefect import flow, get_run_logger, task

from workflows import historical, monitoring, quality


# -------- Realtime Quality Flow -------- #


@task(name="check_new_realtime_data")
def check_new_realtime_data(window_minutes: int = 15) -> List[str]:
    files = quality.discover_new_data(window_minutes)
    get_run_logger().info("Se detectaron %s archivos nuevos", len(files))
    return files


@task(name="comprehensive_quality_checks")
def comprehensive_quality_checks(files: List[str]) -> Dict[str, int]:
    return quality.run_comprehensive_checks(files)


@task(name="calculate_quality_scores")
def calculate_quality_scores(files: List[str]) -> Dict[str, object]:
    staging_path, metrics = quality.score_dataset(files)
    return {"staging_path": staging_path, "metrics": metrics}


@task(name="quality_gate")
def quality_gate(metrics: Dict[str, float]) -> bool:
    return quality.quality_gate(metrics)


@task(name="move_to_silver")
def move_to_silver(staging_path: str) -> str:
    return quality.promote_to_silver(staging_path)


@task(name="update_analytical_tables")
def update_analytical_tables(silver_path: str) -> Dict[str, str]:
    return quality.update_gold_tables(silver_path)


@task(name="generate_quality_report")
def generate_quality_report(metrics: Dict[str, float]) -> str:
    return quality.create_quality_report(metrics)


@flow(name="bike_realtime_quality")
def bike_realtime_quality(window_minutes: int = 15) -> Dict[str, object]:
    logger = get_run_logger()
    files = check_new_realtime_data(window_minutes)
    checks = comprehensive_quality_checks(files)
    payload = calculate_quality_scores(files)
    metrics = payload.get("metrics", {})
    if not metrics.get("records"):
        logger.info("No hay datos nuevos para procesar")
        return {"status": "empty", "metrics": metrics, "checks": checks}

    if not quality_gate(metrics):
        logger.warning("El control de calidad detuvo la promoción: %s", metrics)
        return {"status": "blocked", "metrics": metrics, "checks": checks}

    silver_path = move_to_silver(payload.get("staging_path"))
    gold_outputs = update_analytical_tables(silver_path)
    report_path = generate_quality_report(metrics)
    return {
        "status": "completed",
        "metrics": metrics,
        "checks": checks,
        "silver_path": silver_path,
        "gold_outputs": gold_outputs,
        "report_path": report_path,
    }


# -------- Historical Batch Flow -------- #


@task(name="validate_historical_data")
def validate_historical_data() -> Dict[str, List[str]]:
    return historical.validate_historical_batches()


@task(name="load_to_bronze")
def load_to_bronze(valid_files: List[str]) -> List[str]:
    return historical.load_historical_to_bronze(valid_files)


@task(name="process_to_silver")
def process_to_silver(bronze_files: List[str]) -> Dict[str, object]:
    return historical.process_historical_to_silver(bronze_files)


@flow(name="bike_batch_historical")
def bike_batch_historical() -> Dict[str, object]:
    payload = validate_historical_data()
    bronze_files = load_to_bronze(payload.get("valid_files", []))
    result = process_to_silver(bronze_files)
    return {
        "validation": payload,
        "result": result,
    }


# -------- Monitoring Flow -------- #


@task(name="check_system_health")
def check_system_health() -> Dict[str, object]:
    return monitoring.check_system_health()


@task(name="generate_observability_metrics")
def generate_observability_metrics() -> str:
    return monitoring.generate_observability_metrics()


@task(name="send_alerts_if_needed")
def send_alerts_if_needed(health: Dict[str, object], metrics_path: str) -> None:
    monitoring.send_alerts_if_needed(health, metrics_path)


@flow(name="bike_system_monitoring")
def bike_system_monitoring() -> Dict[str, object]:
    health = check_system_health()
    metrics_path = generate_observability_metrics()
    send_alerts_if_needed(health, metrics_path)
    return {"health": health, "metrics_path": metrics_path}


if __name__ == "__main__":  # pragma: no cover
    bike_realtime_quality()
    bike_batch_historical()
    bike_system_monitoring()
