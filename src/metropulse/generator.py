from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pandas as pd

ZONES = [
    ("Z01", "Central Station", "commercial"),
    ("Z02", "Harbor Front", "commuter"),
    ("Z03", "University", "education"),
    ("Z04", "Museum Quarter", "tourism"),
    ("Z05", "North Market", "residential"),
    ("Z06", "Tech Park", "commercial"),
    ("Z07", "Riverside", "leisure"),
    ("Z08", "Old Town", "tourism"),
]

RIDER_TYPES = ["member", "casual", "corporate"]
VEHICLE_TYPES = ["e-bike", "classic-bike", "scooter"]
PAYMENT_METHODS = ["card", "wallet", "invoice"]


@dataclass(frozen=True)
class RawBatch:
    trips: Path
    payments: Path
    stations: Path
    weather: Path


def generate_raw_batch(
    raw_dir: Path,
    *,
    days: int = 45,
    seed: int = 20260611,
    as_of_date: date | None = None,
) -> RawBatch:
    """Create deterministic raw source files for an inclusive snapshot end date."""

    if days < 2:
        raise ValueError("days must be at least 2")

    raw_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    end_date = as_of_date or date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=days - 1)

    stations_df = _build_stations(rng)
    weather_df = _build_weather(rng, start_date, days)
    trips_df, payments_df = _build_trips_and_payments(
        rng,
        start_date,
        days,
        stations_df,
        weather_df,
    )

    outputs = RawBatch(
        trips=raw_dir / "trips.csv",
        payments=raw_dir / "payments.csv",
        stations=raw_dir / "stations.csv",
        weather=raw_dir / "weather.csv",
    )
    trips_df.to_csv(outputs.trips, index=False)
    payments_df.to_csv(outputs.payments, index=False)
    stations_df.to_csv(outputs.stations, index=False)
    weather_df.to_csv(outputs.weather, index=False)
    return outputs


def _build_stations(rng: random.Random) -> pd.DataFrame:
    rows = []
    station_idx = 1
    for zone_id, zone_name, zone_type in ZONES:
        station_count = 4 if zone_type in {"commercial", "tourism"} else 3
        for local_idx in range(1, station_count + 1):
            rows.append(
                {
                    "station_id": f"S{station_idx:03d}",
                    "station_name": f"{zone_name} Dock {local_idx}",
                    "zone_id": zone_id,
                    "zone_name": zone_name,
                    "zone_type": zone_type,
                    "latitude": round(59.30 + rng.random() * 0.12, 6),
                    "longitude": round(18.00 + rng.random() * 0.16, 6),
                    "capacity": rng.randint(14, 44),
                    "opened_at": f"2024-{rng.randint(1, 12):02d}-{rng.randint(1, 27):02d}",
                }
            )
            station_idx += 1
    return pd.DataFrame(rows)


def _build_weather(rng: random.Random, start_date: date, days: int) -> pd.DataFrame:
    rows = []
    for day_offset in range(days):
        current = start_date + timedelta(days=day_offset)
        seasonal = 16 + 7 * math.sin((current.timetuple().tm_yday / 365) * 2 * math.pi)
        for hour in range(24):
            observed_at = datetime.combine(current, time(hour=hour))
            commute_breeze = 2.5 if hour in {7, 8, 16, 17} else 0
            rain_mm = max(0, rng.gauss(0.35, 0.7))
            if rng.random() > 0.22:
                rain_mm = 0
            temp_c = seasonal + 3.5 * math.sin((hour - 6) / 24 * 2 * math.pi) + rng.gauss(0, 1.2)
            rows.append(
                {
                    "observed_at": observed_at.isoformat(sep=" "),
                    "temp_c": round(temp_c, 1),
                    "rain_mm": round(rain_mm, 2),
                    "wind_kph": round(max(1, rng.gauss(11 + commute_breeze, 4)), 1),
                    "condition": _condition_for_hour(rng, rain_mm),
                }
            )
    return pd.DataFrame(rows)


def _build_trips_and_payments(
    rng: random.Random,
    start_date: date,
    days: int,
    stations_df: pd.DataFrame,
    weather_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    station_ids = stations_df["station_id"].tolist()
    station_to_zone = dict(zip(stations_df["station_id"], stations_df["zone_id"], strict=True))
    zone_weights = {
        "Z01": 1.35,
        "Z02": 1.2,
        "Z03": 1.05,
        "Z04": 1.0,
        "Z05": 0.85,
        "Z06": 1.25,
        "Z07": 0.75,
        "Z08": 0.95,
    }
    weather_by_hour = {row.observed_at: row for row in weather_df.itertuples(index=False)}
    trips: list[dict[str, object]] = []
    payments: list[dict[str, object]] = []
    trip_number = 1

    for day_offset in range(days):
        current = start_date + timedelta(days=day_offset)
        weekday_factor = 1.18 if current.weekday() < 5 else 0.78
        for hour in range(24):
            observed_at = datetime.combine(current, time(hour=hour)).isoformat(sep=" ")
            weather = weather_by_hour[observed_at]
            hour_factor = _hour_factor(hour)
            weather_factor = _weather_factor(weather.condition)
            expected = 10.5 * weekday_factor * hour_factor * weather_factor
            trip_count = max(1, int(rng.gauss(expected, max(2.0, expected * 0.16))))

            for _ in range(trip_count):
                start_station = _weighted_station(rng, stations_df, station_to_zone, zone_weights)
                end_candidates = [station for station in station_ids if station != start_station]
                end_station = rng.choice(end_candidates)
                started_at = datetime.combine(current, time(hour=hour, minute=rng.randint(0, 59)))
                distance = max(0.35, rng.lognormvariate(0.8, 0.45))
                duration = max(3.0, distance * rng.uniform(4.5, 8.5) + rng.gauss(0, 2.0))
                ended_at = started_at + timedelta(minutes=duration)
                rider_type = rng.choices(RIDER_TYPES, weights=[0.64, 0.28, 0.08], k=1)[0]
                vehicle_type = rng.choices(VEHICLE_TYPES, weights=[0.46, 0.42, 0.12], k=1)[0]

                base_fare = 1.8 if vehicle_type != "scooter" else 2.25
                rider_discount = 0.85 if rider_type == "member" else 1.0
                weather_surcharge = 0.45 if weather.condition == "rain" else 0
                fare_amount = (
                    base_fare + distance * 1.25 + duration * 0.035 + weather_surcharge
                ) * rider_discount
                discount_amount = 0 if rider_type != "member" else fare_amount * 0.08
                tax_amount = fare_amount * 0.12
                total_amount = fare_amount - discount_amount + tax_amount

                trip_id = f"T{trip_number:08d}"
                trips.append(
                    {
                        "trip_id": trip_id,
                        "started_at": started_at.isoformat(sep=" "),
                        "ended_at": ended_at.isoformat(sep=" "),
                        "start_station_id": start_station,
                        "end_station_id": end_station,
                        "rider_type": rider_type,
                        "vehicle_type": vehicle_type,
                        "distance_km": round(distance, 2),
                        "duration_min": round(duration, 1),
                    }
                )
                payments.append(
                    {
                        "payment_id": f"P{trip_number:08d}",
                        "trip_id": trip_id,
                        "fare_amount": round(fare_amount, 2),
                        "discount_amount": round(discount_amount, 2),
                        "tax_amount": round(tax_amount, 2),
                        "total_amount": round(total_amount, 2),
                        "payment_method": rng.choices(
                            PAYMENT_METHODS,
                            weights=[0.66, 0.28, 0.06],
                            k=1,
                        )[0],
                        "paid_at": (
                            ended_at + timedelta(seconds=rng.randint(8, 360))
                        ).isoformat(sep=" "),
                    }
                )
                trip_number += 1

    return pd.DataFrame(trips), pd.DataFrame(payments)


def _hour_factor(hour: int) -> float:
    if hour in {7, 8, 16, 17, 18}:
        return 2.4
    if hour in {11, 12, 13, 14, 15}:
        return 1.25
    if hour in {22, 23, 0, 1, 2, 3, 4, 5}:
        return 0.28
    return 0.9


def _condition_for_hour(rng: random.Random, rain_mm: float) -> str:
    if rain_mm >= 0.8:
        return "rain"
    if rng.random() < 0.32:
        return "cloudy"
    return "clear"


def _weather_factor(condition: str) -> float:
    if condition == "rain":
        return 0.72
    if condition == "cloudy":
        return 0.93
    return 1.0


def _weighted_station(
    rng: random.Random,
    stations_df: pd.DataFrame,
    station_to_zone: dict[str, str],
    zone_weights: dict[str, float],
) -> str:
    station_ids = stations_df["station_id"].tolist()
    weights = [zone_weights[station_to_zone[station]] for station in station_ids]
    return rng.choices(station_ids, weights=weights, k=1)[0]
