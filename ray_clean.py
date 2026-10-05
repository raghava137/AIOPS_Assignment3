"""
ray_clean.py - NYC Taxi cleaning pipeline on Ray Data (DA3408 Assignment 3).
Same steps and rules as spark_clean.py.

Run on node0:
  python3 ray_clean.py --input /data/raw --zones /data/raw/taxi_zone_lookup.csv --output /data/out/ray
"""
import argparse
import csv
import glob
import os
import shutil
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import ray
from ray.data.aggregate import Count, Sum

# Columns we use and the type each is cast to (TLC files from different years use different types).
COLUMNS = {
    "VendorID": pa.int64(),
    "tpep_pickup_datetime": pa.timestamp("us"),
    "tpep_dropoff_datetime": pa.timestamp("us"),
    "passenger_count": pa.int64(),
    "trip_distance": pa.float64(),
    "PULocationID": pa.int64(),
    "DOLocationID": pa.int64(),
    "payment_type": pa.int64(),
    "fare_amount": pa.float64(),
    "total_amount": pa.float64(),
}

OUTPUT = ["VendorID", "passenger_count", "trip_distance", "PULocationID", "DOLocationID",
          "payment_type", "fare_amount", "total_amount", "pickup_date", "pickup_hour",
          "pickup_dow", "duration_sec", "pickup_borough", "pickup_zone",
          "dropoff_borough", "dropoff_zone", "avg_speed_mph"]


def avg_speed_mph(distance, duration_sec):
    """The custom Python UDF. spark_clean.py has an identical copy."""
    if distance is None or duration_sec is None or distance <= 0 or duration_sec <= 0:
        return None
    speed = distance / (duration_sec / 3600.0)
    return round(speed, 4) if speed <= 100 else None


def cast_columns(t: pa.Table) -> pa.Table:
    return pa.table({c: pc.cast(t[c], typ, safe=False) for c, typ in COLUMNS.items()})


def drop_nulls(t: pa.Table) -> pa.Table:
    t = t.drop_null()
    for c in ("trip_distance", "fare_amount", "total_amount"):   # Spark's na.drop() also drops NaN
        t = t.filter(pc.invert(pc.is_nan(t[c])))
    return t


def filter_and_format(t: pa.Table) -> pa.Table:
    def seconds(c):   # timestamp -> whole seconds since 1970 (same as Spark's cast to long)
        return pc.cast(pc.cast(t[c], pa.timestamp("s"), safe=False), pa.int64())

    t = t.append_column("duration_sec", pc.subtract(seconds("tpep_dropoff_datetime"),
                                                    seconds("tpep_pickup_datetime")))
    t = t.filter((pc.field("trip_distance") > 0) & (pc.field("trip_distance") < 200)
                 & (pc.field("duration_sec") > 0) & (pc.field("duration_sec") <= 6 * 3600)
                 & (pc.field("fare_amount") >= 0) & (pc.field("total_amount") >= 0))
    ts = t["tpep_pickup_datetime"]
    t = t.append_column("pickup_date", pc.strftime(ts, format="%Y-%m-%d"))
    t = t.append_column("pickup_hour", pc.cast(pc.hour(ts), pa.int64()))
    t = t.append_column("pickup_dow", pc.cast(               # 1=Sunday .. 7=Saturday, like Spark
        pc.day_of_week(ts, count_from_zero=False, week_start=7), pa.int64()))
    return t


def join_zones(df: pd.DataFrame, pu: pd.DataFrame, do: pd.DataFrame) -> pd.DataFrame:
    """Broadcast join: every worker gets a copy of the small zone table (pu, do)."""
    return df.merge(pu, on="PULocationID", how="inner").merge(do, on="DOLocationID", how="inner")


def add_speed(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the Python UDF row by row. Data is already in Python, no JVM in between."""
    speeds = pd.Series([avg_speed_mph(d, s) for d, s in
                        zip(df["trip_distance"].tolist(), df["duration_sec"].tolist())],
                       index=df.index, dtype="float64")
    df["avg_speed_mph"] = speeds
    return df.loc[speeds.notna(), OUTPUT]


def speed_scaled(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({"pickup_hour": df["pickup_hour"],
                         "s": np.rint(df["avg_speed_mph"] * 10000).astype("int64")})


def hourly_average(df: pd.DataFrame) -> pd.DataFrame:
    df["avg_speed_mph"] = df["s"] / df["trip_count"] / 10000
    return df[["pickup_hour", "trip_count", "avg_speed_mph"]]


def numpy_version(b):
    return {"s": [np.round(b["trip_distance"] / (b["duration_sec"] / 3600.0), 4).sum()]}


def python_version(b):
    v = [avg_speed_mph(d, s) for d, s in zip(b["trip_distance"].tolist(), b["duration_sec"].tolist())]
    return {"s": [sum(x for x in v if x is not None)]}


def timed(fn):
    t = time.perf_counter()
    fn()
    return round(time.perf_counter() - t, 2)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="folder with the trip .parquet files")
    p.add_argument("--zones", required=True, help="taxi_zone_lookup.csv")
    p.add_argument("--output", required=True, help="output folder")
    p.add_argument("--results", default="results", help="folder for runs.csv")
    p.add_argument("--address", default="auto", help="Ray cluster address")
    p.add_argument("--partitions", type=int, default=8, help="shuffle partitions (same as Spark)")
    p.add_argument("--max-aggregators", type=int, default=0, help="0 = decide automatically")
    a = p.parse_args()

    ray.init(address=a.address)
    ctx = ray.data.DataContext.get_current()
    ctx.default_hash_shuffle_parallelism = a.partitions
    # Removing duplicates is a shuffle done by "aggregator" processes, each reserving at least
    # 1 GiB of memory. Start only as many as fit in the two workers' memory, or the job hangs.
    if a.max_aggregators:
        ctx.max_hash_shuffle_aggregators = a.max_aggregators
    else:
        worker_mem = sum(n["Resources"].get("memory", 0) for n in ray.nodes()
                         if n["Alive"] and n["Resources"].get("CPU", 0) > 0)
        ctx.max_hash_shuffle_aggregators = max(1, min(a.partitions, int(worker_mem * 0.8 // 2**30)))
    shutil.rmtree(a.output, ignore_errors=True)   # Ray adds files instead of overwriting
    trips_path = os.path.join(a.output, "trips")
    hourly_path = os.path.join(a.output, "avg_speed_per_hour")
    start = time.perf_counter()

    # 1. Ingestion: read each file, cast to the same types, combine
    files = sorted(glob.glob(os.path.join(a.input, "*.parquet")))
    parts = [ray.data.read_parquet(f).select_columns(list(COLUMNS))
             .map_batches(cast_columns, batch_format="pyarrow") for f in files]
    ds = parts[0].union(*parts[1:]) if len(parts) > 1 else parts[0]
    zones = pd.read_csv(a.zones, keep_default_na=False)

    # 2. Cleansing: nulls, duplicates (= group by every column), impossible trips, timestamps
    ds = ds.map_batches(drop_nulls, batch_format="pyarrow")
    ds = ds.groupby(list(COLUMNS)).count().drop_columns(["count()"])
    ds = ds.map_batches(filter_and_format, batch_format="pyarrow")

    # 3. Transformation: join with the zone table (pickup + dropoff), then the Python UDF.
    #    The zone table is tiny, so it is broadcast: a copy goes to every worker and the
    #    big trip table never has to be shuffled across the network.
    pu = zones.rename(columns={"LocationID": "PULocationID", "Borough": "pickup_borough",
                               "Zone": "pickup_zone"})[["PULocationID", "pickup_borough", "pickup_zone"]]
    do = zones.rename(columns={"LocationID": "DOLocationID", "Borough": "dropoff_borough",
                               "Zone": "dropoff_zone"})[["DOLocationID", "dropoff_borough", "dropoff_zone"]]
    ds = ds.map_batches(join_zones, batch_format="pandas", fn_kwargs={"pu": pu, "do": do})
    ds = ds.map_batches(add_speed, batch_format="pandas")

    # 4. Export the cleaned trips
    ds.write_parquet(trips_path)

    # Average speed per pickup hour, computed from the exported trips (exact whole-number sum,
    # same method as Spark)
    hourly = (ray.data.read_parquet(trips_path).select_columns(["pickup_hour", "avg_speed_mph"])
              .map_batches(speed_scaled, batch_format="pandas")
              .groupby("pickup_hour")
              .aggregate(Count(alias_name="trip_count"), Sum("s", alias_name="s"))
              .map_batches(hourly_average, batch_format="pandas"))
    hourly.write_parquet(hourly_path)
    total = round(time.perf_counter() - start, 2)
    rows = ray.data.read_parquet(trips_path).count()
    print(f"Ray: {len(files)} files, {rows} rows written, total time {total} s")

    # UDF overhead: same formula vectorised with NumPy vs. the Python UDF row by row
    base = ray.data.read_parquet(trips_path).select_columns(["trip_distance", "duration_sec"]).materialize()
    native_s = timed(lambda: base.map_batches(numpy_version, batch_format="numpy").take_all())
    python_s = timed(lambda: base.map_batches(python_version, batch_format="numpy").take_all())
    print(f"Ray UDF: NumPy {native_s} s, Python UDF {python_s} s")

    os.makedirs(a.results, exist_ok=True)
    path = os.path.join(a.results, "runs.csv")
    new = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["framework", "time", "files", "rows", "total_s", "udf_native_s", "udf_python_s"])
        w.writerow(["ray", time.strftime("%Y-%m-%d %H:%M"), len(files), rows, total, native_s, python_s])


if __name__ == "__main__":
    main()
