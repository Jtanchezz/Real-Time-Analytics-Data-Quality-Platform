#!/usr/bin/env python3
"""Generador simple para bombardear la API con eventos controlados."""
import argparse
import asyncio
import itertools
import random
import time
import uuid
from datetime import datetime, timedelta

import httpx


def build_event(idx: int, variant: str) -> dict:
    now = datetime.utcnow()
    start = now - timedelta(minutes=random.randint(0, 5))
    duration_seconds = random.randint(300, 3600)
    end = start + timedelta(seconds=duration_seconds)
    payload = {
        "trip_id": f"load-{idx}-{uuid.uuid4().hex[:6]}",
        "bike_id": idx,
        "start_time": start.isoformat() + "Z",
        "end_time": end.isoformat() + "Z",
        "start_station_id": random.choice([101.0, 102.0, 99999.0]),
        "end_station_id": random.choice([201.0, 202.0, 88888.0]),
        "rider_age": random.randint(16, 80),
        "trip_duration": int((end - start).total_seconds()),
        "bike_type": random.choice(["electric", "classic"]),
        "member_casual": random.choice(["member", "casual"]),
    }

    if variant == "missing_fields":
        payload.pop("bike_type")
        payload["end_time"] = payload["start_time"]
    elif variant == "type_errors":
        payload["rider_age"] = "NaN"
        payload["trip_duration"] = -50
    elif variant == "speeding":
        payload["start_station_id"] = 123
        payload["end_station_id"] = 456
        payload["trip_duration"] = 30
    return payload


async def fire(rate: float, duration: int, url: str, bad_ratio: float) -> None:
    interval = 1.0 / rate
    stop_at = time.time() + duration
    success = errors = 0
    variants = ["clean", "missing_fields", "type_errors", "speeding"]
    async with httpx.AsyncClient(timeout=5) as client:
        for idx in itertools.count():
            if time.time() > stop_at:
                break
            variant = "clean"
            if random.random() < bad_ratio:
                variant = random.choice(variants[1:])
            payload = build_event(idx, variant)
            try:
                resp = await client.post(url, json=payload)
                if resp.status_code == 200:
                    success += 1
                else:
                    errors += 1
            except Exception:
                errors += 1
            await asyncio.sleep(interval)
    total = success + errors
    print(f"enviados={total} ok={success} errores={errors}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000/api/v1/trips")
    parser.add_argument("--rate", type=float, default=5.0)
    parser.add_argument("--duration", type=int, default=600)
    parser.add_argument(
        "--bad-ratio", type=float, default=0.3, help="Fracción de eventos con errores"
    )
    args = parser.parse_args()
    asyncio.run(fire(args.rate, args.duration, args.url, args.bad_ratio))


if __name__ == "__main__":
    main()
