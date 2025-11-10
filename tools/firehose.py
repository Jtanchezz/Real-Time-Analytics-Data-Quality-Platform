#!/usr/bin/env python3
"""Generador simple para bombardear la API con eventos controlados."""
import argparse
import asyncio
import itertools
import time
import uuid
from datetime import datetime, timedelta

import httpx


def build_event(idx: int) -> dict:
    start = datetime.utcnow()
    end = start + timedelta(minutes=45)
    return {
        "trip_id": f"load-{idx}-{uuid.uuid4().hex[:6]}",
        "bike_id": f"bike-{idx}",
        "start_time": start.isoformat() + "Z",
        "end_time": end.isoformat() + "Z",
        "start_station_id": 123,
        "end_station_id": 456,
        "rider_age": 30,
        "trip_duration": int((end - start).total_seconds()),
        "bike_type": "electric",
    }


async def fire(rate: float, duration: int, url: str) -> None:
    interval = 1.0 / rate
    stop_at = time.time() + duration
    success = errors = 0
    async with httpx.AsyncClient(timeout=5) as client:
        for idx in itertools.count():
            if time.time() > stop_at:
                break
            payload = build_event(idx)
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
    args = parser.parse_args()
    asyncio.run(fire(args.rate, args.duration, args.url))


if __name__ == "__main__":
    main()
