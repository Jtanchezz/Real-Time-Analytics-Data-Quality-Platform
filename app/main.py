from __future__ import annotations

import logging
from time import perf_counter

from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .config import get_settings
from .logging_config import configure_logging
from .metrics import IngestionMetrics
from .schemas import TripEvent, TripIngestResponse
from .storage import BronzeWriter

configure_logging()
logger = logging.getLogger("bike_api")
settings = get_settings()
metrics = IngestionMetrics(settings.metrics_window_seconds)
bronze_writer = BronzeWriter(settings)

app = FastAPI(title=settings.app_name)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    metrics.record_error()
    logger.warning("Validation error: %s", exc.errors())
    return JSONResponse(
        status_code=400,
        content={
            "status": "error",
            "message": "Invalid payload",
            "details": exc.errors(),
        },
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    metrics.record_error()
    logger.exception("Unhandled API error")
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "message": "Internal server error",
        },
    )


@app.post("/api/v1/trips", response_model=TripIngestResponse)
async def ingest_trip(
    trip: TripEvent, background_tasks: BackgroundTasks
) -> TripIngestResponse:
    start = perf_counter()
    payload = trip.model_dump(mode="json")
    background_tasks.add_task(bronze_writer.persist_event, payload)
    latency_ms = (perf_counter() - start) * 1000
    metrics.record_event(latency_ms)
    logger.info("Trip %s accepted", trip.trip_id)
    return TripIngestResponse(
        status="received",
        trip_id=trip.trip_id,
        message="Event accepted for processing",
    )


@app.get("/api/v1/metrics")
async def get_metrics() -> dict:
    return metrics.as_dict()


@app.get("/health")
async def healthcheck() -> dict:
    return {"status": "ok"}


@app.middleware("http")
async def enforce_rate_limit(request: Request, call_next):
    if request.url.path != "/api/v1/trips":
        return await call_next(request)

    if metrics.snapshot().events_last_minute > (settings.max_events_per_second * 60):
        metrics.record_error()
        logger.error("Throughput threshold hit, rejecting request")
        return JSONResponse(
            status_code=429,
            content={
                "status": "error",
                "message": "Throughput exceeded. Try again shortly.",
            },
        )

    response = await call_next(request)
    return response


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
