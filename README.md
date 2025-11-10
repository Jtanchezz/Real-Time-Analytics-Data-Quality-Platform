# Proyecto3DataEng

Plataforma de datos para viajes en bici con API de ingesta de baja latencia, lago en MinIO/S3 y orquestación en Prefect.

## 1. Prerrequisitos
- Python 3.11+
- pip
- Docker Desktop (para MinIO)
- Prefect CLI (se instala con `pip install -r requirements.txt`)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### MinIO (S3 local)
```bash
# servidor MinIO
docker run -d --name minio \
  -p 9000:9000 -p 9090:9090 \
  -e MINIO_ROOT_USER=admin \
  -e MINIO_ROOT_PASSWORD=admin123 \
  -v $(pwd)/minio-data:/data \
  minio/minio server /data --console-address :9090

# crear buckets
docker run --rm --entrypoint /bin/sh minio/mc -c "
  mc alias set local http://host.docker.internal:9000 admin admin123 &&
  mc mb -p local/bronze &&
  mc mb -p local/silver &&
  mc mb -p local/gold &&
  mc mb -p local/historical
"
```
La API lee credenciales desde `.env` (ya incluye valores por defecto para este entorno).

## 2. API de Ingesta
FastAPI recibe hasta 10 eventos/seg con <100 ms de respuesta y persiste cada payload inmediatamente en `s3://bronze/date=YYYY-MM-DD/hour=HH/` como Parquet + JSON.

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

- `POST /api/v1/trips` (ver ejemplo en `app/schemas.py`).
- `GET /api/v1/metrics` devuelve eventos/min, errores/min y latencia promedio.
- `GET /health` para probes.

## 3. Capas del lago
| Capa | Ubicación | Descripción |
|------|-----------|-------------|
| Bronze | `s3://bronze/` | Datos crudos con particiones `date=`/`hour=`. Los backfills validados caen bajo `historical/`. |
| Silver | `s3://silver/` | Datos validados con `quality_score`, `quality_band`, `deductions`. |
| Gold | `s3://gold/` | Tablas analíticas (`station_status.parquet`, `hourly_usage.parquet`, `quality_trends.parquet`) y reportes JSON (`reports/`). |
| Historical | `s3://historical/` | Staging para lotes (`pending/` → QA → `processed/`). |

`reference/stations_reference.csv` permanece local y alimenta las reglas de velocidad.

## 4. Marco de calidad
`Quality_Score = max(0, 100 - Deductions)` con bandas: ≥90 EXCELLENT, 75-89 GOOD, 60-74 FAIR, <60 POOR.

| Categoría | Regla | Penalidad |
|-----------|-------|-----------|
| **Schema & Completeness** | Campo requerido faltante | -10 c/u |
| | Tipo inválido | -5 c/u |
| | Campo inesperado | -2 c/u |
| **Data Validity** | `start_time ≥ end_time` | -25 |
| | `trip_duration` no cuadra (>60s) | -15 |
| | `rider_age` <16 o >100 | -20 |
| | Estación inexistente | -10 c/u |
| **Business Logic** | Duración <60s o >24h | -15 |
| | Velocidad >30 km/h | -10 |
| | Misma estación origen/destino >1h | -5 |

Los resultados se escriben directamente en S3: `s3://silver/date=.../hour=.../silver_*.parquet` y los agregados/ reportes en `s3://gold/...`.

## 5. Orquestación con Prefect
`prefect_flows/bike_data_platform.py` define los tres flujos principales:

1. **`bike_realtime_quality`** (cada 10 min): `check_new_realtime_data → comprehensive_quality_checks → calculate_quality_scores → quality_gate → move_to_silver → update_analytical_tables → generate_quality_report`.
2. **`bike_batch_historical`** (diario): `validate_historical_data → load_to_bronze → process_to_silver`.
3. **`bike_system_monitoring`** (cada hora): `check_system_health → generate_observability_metrics → send_alerts_if_needed`.

### Ejecución ad-hoc
```bash
# correr los tres flows una vez
python -m prefect_flows.bike_data_platform

# o ejecutar uno puntual con la CLI de Prefect
prefect run python -n bike_realtime_quality prefect_flows/bike_data_platform.py
```

### Crear deployments y scheduler
```bash
prefect deployment build prefect_flows/bike_data_platform.py:bike_realtime_quality \
  -n realtime-quality -q default -o deployments/realtime_quality.yaml
prefect deployment apply deployments/realtime_quality.yaml
prefect agent start -q default
```
(Repite para los otros dos flujos con el cron deseado.)

## 6. Observabilidad
- **Ingesta:** `/api/v1/metrics` puede exponerse vía Prometheus/Grafana.
- **Calidad:** Usa `s3://gold/quality_trends.parquet` + los reportes de `s3://gold/reports/`.
- **Salud del pipeline:** `monitoring.generate_observability_metrics()` escribe `s3://gold/dashboards/pipeline_metrics.json`. Las alertas siguen en `logs/alerts.log` (útil para reenviar a Slack/Email).

## 7. Validaciones rápidas
```bash
python3 - <<'PY'
from fastapi.testclient import TestClient
from app.main import app
client = TestClient(app)
payload = {
    "trip_id": "abc123",
    "bike_id": "12345",
    "start_time": "2024-01-15T08:30:00Z",
    "end_time": "2024-01-15T09:15:00Z",
    "start_station_id": 123,
    "end_station_id": 456,
    "rider_age": 28,
    "trip_duration": 2700,
    "bike_type": "electric"
}
print(client.post("/api/v1/trips", json=payload).json())
print(client.get("/api/v1/metrics").json())
PY
```

## 8. Stress test local
Envía tráfico sintético con el script incluido:
```bash
python3 tools/firehose.py --rate 5 --duration 900  # 5 eventos/s durante 15 min
```
Compara el contador final del script con la cantidad de objetos en `s3://bronze` para asegurar que no hubo pérdidas.

## 9. Próximos pasos
1. Montar dashboards en Grafana/Prometheus usando `/api/v1/metrics` y los artefactos de Gold.
2. Automatizar los deployments de Prefect vía CI/CD y habilitar alertas (Slack/PagerDuty).
3. Integrar `workflows/monitoring.send_alerts_if_needed` con tu sistema de notificaciones y versionar las estaciones en una tabla reference centralizada.
