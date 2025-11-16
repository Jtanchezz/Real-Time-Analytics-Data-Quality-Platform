from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, confloat, conint, constr


class TripEvent(BaseModel):
    trip_id: constr(min_length=1)
    bike_id: conint(ge=0)
    start_time: datetime = Field(description="ISO8601 timestamp for trip start")
    end_time: datetime = Field(description="ISO8601 timestamp for trip end")
    start_station_id: confloat(ge=0)
    end_station_id: confloat(ge=0)
    rider_age: conint(ge=12, le=110)
    trip_duration: conint(gt=0)
    bike_type: constr(min_length=1)
    member_casual: constr(min_length=1)

    class Config:
        json_schema_extra = {
            "example": {
                "trip_id": "abc123",
                "bike_id": 12345,
                "start_time": "2024-01-15T08:30:00Z",
                "end_time": "2024-01-15T09:15:00Z",
                "start_station_id": 123.0,
                "end_station_id": 456.0,
                "rider_age": 28,
                "trip_duration": 2700,
                "bike_type": "electric",
                "member_casual": "member",
            }
        }


class TripIngestResponse(BaseModel):
    status: Literal["received", "error"] = "received"
    trip_id: str
    message: str
