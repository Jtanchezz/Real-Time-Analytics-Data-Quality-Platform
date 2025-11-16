# Observabilidad y Dashboards

Estos tableros consumen únicamente artefactos que ya produce la plataforma (sin consultas directas a MinIO). Puedes montarlos en Grafana, Superset o incluso en un panel ligero en Streamlit/React.

## Fuentes de datos

| Fuente | Ubicación | Contenido |
|--------|-----------|-----------|
| API Metrics | `GET ${APP_METRICS_API_URL}/api/v1/metrics` | Eventos/min, errores/min y latencia promedio reportados por FastAPI en tiempo real. |
| Pipeline Snapshot | `s3://gold/dashboards/pipeline_metrics.json` | Métricas agregadas generadas por el flow `bike_system_monitoring`: estado de la API, conteo de objetos por capa, último `quality_report` y estados de los flows. |
| Quality Reports | `s3://gold/reports/quality_report_*.json` | Resultado detallado de cada corrida de QA (records, pass-rate, poor_share, promedios de deducciones, bandas). |
| Quality Trends | `s3://gold/quality_trends.parquet` | Tendencia diaria de Quality Score y share de la banda POOR. |
| Flow Status | `s3://gold/dashboards/flow_status/*.json` | Último estado conocido por flow Prefect (status, métricas asociadas, timestamp). |
| Alert Feed | `logs/alerts.log` | Registro histórico de alertas disparadas localmente (útil para integrarlo con Slack/Webhooks). |

## Dashboard 1 – API Ingest (near real time)

Objetivo: garantizar cero data loss y latencia <100 ms.

Widgets sugeridos:

1. **Events per minute** (line chart) usando `/api/v1/metrics.events_per_minute`.
2. **Avg latency vs objetivo** (area chart) comparando `avg_latency_ms` contra el SLA.
3. **Errores por minuto / tasa de error** (bar + single stat) a partir de `errors_per_minute`.
4. **Estado de la API** (single stat) con `pipeline_metrics.api.reachable` y último timestamp.

Alertas asociadas:
- API down → `api.reachable == False`.
- Latencia > 200 ms durante 5 min consecutivos.

## Dashboard 2 – Calidad de Datos

Objetivo: monitorear `quality_score`, deducciones y patrones de fallas.

Widgets sugeridos:

1. **Quality Score trend** (line) usando `quality_trends.parquet.mean_quality`.
2. **Bandas de calidad** (stacked bar) usando `quality_trends.parquet.poor_share` y `quality_report.band_counts`.
3. **Histograma de deducciones** con `quality_report.schema_penalty_avg`, `validity_penalty_avg`, `business_penalty_avg`.
4. **Top reglas que fallan** (table) derivada de `silver` (columnas `schema_penalty`, `validity_penalty`, `business_penalty`).

Alertas asociadas:
- `quality_report.poor_share > 0.20` → enviar alerta (ya queda registrada en `alerts.log`).

## Dashboard 3 – Historical vs Realtime Processing

Objetivo: asegurar que los dos flows alimentan correctamente Bronze/Silver/Gold.

Widgets:

1. **Backlog histórico**: número de archivos pendientes (`len(validate_historical_data.valid_files)`).
2. **Bronze/Silver object count**: usa `pipeline_metrics.storage`.
3. **Tiempo de procesamiento por flow**: lee `flow_status/*.json` y grafica duración/estado.
4. **Última corrida**: tabla con `flow_status` para `bike_realtime_quality`, `bike_batch_historical`, `bike_system_monitoring`.

Alertas:
- Algún flow distinto de `completed`/`empty`.
- `pending` histórico > 0 durante más de N horas.

## Dashboard 4 – Health & Alerts

Objetivo: tener una vista unificada de incidentes.

Widgets:

1. **Alert timeline** leyendo `logs/alerts.log`.
2. **Estado de pipelines** (traffic lights) usando `flow_status`.
3. **Latency vs Throughput** superpuesta para detectar throttling.

## Cómo enganchar las alertas

1. **API down**: `monitoring.send_alerts_if_needed` ya lo registra; agrega un watcher que tailée `logs/alerts.log` y publique en Slack.
2. **>20% POOR**: hook directo sobre `quality_report.poor_share`.
3. **Pipeline failure**: lee `flow_status/*.json` y dispara cuando `status` ∈ {`blocked`, `failed`, `degraded`}.

## Buenas prácticas

- Mantén `APP_METRICS_API_URL` apuntando al endpoint accesible por Grafana.
- Da permisos de solo lectura a los artefactos de Gold para tus herramientas de dashboarding.
- Versiona tus dashboards como código (ej. `grafana/provisioning`) para reproducibilidad.
