"""Generate small synthetic Parquet fixtures that mimic TLC schemas (NOT real TLC data).

2024 files use the older schema; 2025 files add `cbd_congestion_fee`. Values are random with a
fixed seed. Deliberate defects are injected so quality rules have something to catch.
"""
from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

BASE_COLS = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
             "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID",
             "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
             "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee"]


def make_month(year: int, month: int, rows: int, seed: int, with_cbd: bool, defects: bool) -> pa.Table:
    rng = random.Random(seed)
    start = datetime(year, month, 1)
    data: dict[str, list] = {c: [] for c in BASE_COLS}
    if with_cbd:
        data["cbd_congestion_fee"] = []
    for i in range(rows):
        pickup = start + timedelta(minutes=rng.randint(0, 27 * 24 * 60))
        dropoff = pickup + timedelta(minutes=rng.randint(3, 60))
        fare = round(rng.uniform(5, 60), 2)
        tip = round(fare * rng.choice([0, 0.1, 0.2]), 2)
        row = {
            "VendorID": rng.choice([1, 2]), "tpep_pickup_datetime": pickup,
            "tpep_dropoff_datetime": dropoff, "passenger_count": float(rng.randint(1, 4)),
            "trip_distance": round(rng.uniform(0.3, 15), 2), "RatecodeID": float(rng.choice([1, 1, 1, 2])),
            "store_and_fwd_flag": "N", "PULocationID": rng.randint(1, 263),
            "DOLocationID": rng.randint(1, 263), "payment_type": rng.choice([1, 1, 2, 3]),
            "fare_amount": fare, "extra": 1.0, "mta_tax": 0.5, "tip_amount": tip, "tolls_amount": 0.0,
            "improvement_surcharge": 1.0, "total_amount": round(fare + tip + 2.5, 2),
            "congestion_surcharge": 2.5, "airport_fee": 0.0,
        }
        if with_cbd:
            row["cbd_congestion_fee"] = 0.75
        if defects and i == 0:
            row["tpep_dropoff_datetime"] = pickup - timedelta(minutes=5)   # dropoff before pickup
        if defects and i == 1:
            row["trip_distance"] = -3.0                                     # negative distance
        if defects and i == 2:
            row["PULocationID"] = 999                                       # zone not in lookup
        if defects and i == 3:
            row["tpep_pickup_datetime"] = None                              # missing pickup
        for key, value in row.items():
            data[key].append(value)
    schema = pa.schema([
        ("VendorID", pa.int32()), ("tpep_pickup_datetime", pa.timestamp("us")),
        ("tpep_dropoff_datetime", pa.timestamp("us")), ("passenger_count", pa.float64()),
        ("trip_distance", pa.float64()), ("RatecodeID", pa.float64()), ("store_and_fwd_flag", pa.string()),
        ("PULocationID", pa.int32()), ("DOLocationID", pa.int32()), ("payment_type", pa.int64()),
        ("fare_amount", pa.float64()), ("extra", pa.float64()), ("mta_tax", pa.float64()),
        ("tip_amount", pa.float64()), ("tolls_amount", pa.float64()),
        ("improvement_surcharge", pa.float64()), ("total_amount", pa.float64()),
        ("congestion_surcharge", pa.float64()), ("airport_fee", pa.float64()),
    ] + ([("cbd_congestion_fee", pa.float64())] if with_cbd else []))
    return pa.table(data, schema=schema)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--periods", nargs="+", default=["2024-12", "2025-01", "2025-02"])
    parser.add_argument("--rows", type=int, default=2000)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    for n, period in enumerate(args.periods):
        year, month = map(int, period.split("-"))
        table = make_month(year, month, args.rows, seed=100 + n, with_cbd=year >= 2025, defects=True)
        pq.write_table(table, args.out / f"yellow_tripdata_{period}.parquet")
        print("wrote", period, table.num_rows, "rows")


if __name__ == "__main__":
    main()
