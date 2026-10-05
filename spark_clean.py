"""
spark_clean.py - NYC Taxi cleaning pipeline on Apache Spark (DA3408 Assignment 3).

Run on node0:
  spark-submit --master spark://node0:7077 --executor-memory 3g spark_clean.py \
      --input /data/raw --zones /data/raw/taxi_zone_lookup.csv --output /data/out/spark
"""
import argparse
import csv
import glob
import os
import time

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Columns we use and the type each is cast to (TLC files from different years use different types).
COLUMNS = {
    "VendorID": "long",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "long",
    "trip_distance": "double",
    "PULocationID": "long",
    "DOLocationID": "long",
    "payment_type": "long",
    "fare_amount": "double",
    "total_amount": "double",
}

OUTPUT = ["VendorID", "passenger_count", "trip_distance", "PULocationID", "DOLocationID",
          "payment_type", "fare_amount", "total_amount", "pickup_date", "pickup_hour",
          "pickup_dow", "duration_sec", "pickup_borough", "pickup_zone",
          "dropoff_borough", "dropoff_zone", "avg_speed_mph"]


def avg_speed_mph(distance, duration_sec):
    """The custom Python UDF. ray_clean.py has an identical copy."""
    if distance is None or duration_sec is None or distance <= 0 or duration_sec <= 0:
        return None
    speed = distance / (duration_sec / 3600.0)
    return round(speed, 4) if speed <= 100 else None


speed_udf = F.udf(avg_speed_mph, T.DoubleType())


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
    p.add_argument("--partitions", type=int, default=8, help="shuffle partitions (same as Ray)")
    a = p.parse_args()

    spark = (SparkSession.builder.appName("a3-spark-clean")
             .config("spark.sql.session.timeZone", "UTC")             # same clock as Ray
             .config("spark.sql.shuffle.partitions", a.partitions)
             .getOrCreate())
    trips_path = os.path.join(a.output, "trips")
    hourly_path = os.path.join(a.output, "avg_speed_per_hour")
    start = time.perf_counter()

    # 1. Ingestion: read each file, cast to the same types, combine
    files = sorted(glob.glob(os.path.join(a.input, "*.parquet")))
    trips = None
    for f in files:
        df = spark.read.parquet(f).select([F.col(c).cast(t) for c, t in COLUMNS.items()])
        trips = df if trips is None else trips.unionByName(df)
    zones = spark.createDataFrame(pd.read_csv(a.zones, keep_default_na=False))

    # 2. Cleansing: nulls, duplicates, impossible trips, timestamp formatting
    trips = trips.na.drop().dropDuplicates()
    trips = trips.withColumn("duration_sec", F.col("tpep_dropoff_datetime").cast("long")
                             - F.col("tpep_pickup_datetime").cast("long"))
    trips = trips.filter((F.col("trip_distance") > 0) & (F.col("trip_distance") < 200)
                         & (F.col("duration_sec") > 0) & (F.col("duration_sec") <= 6 * 3600)
                         & (F.col("fare_amount") >= 0) & (F.col("total_amount") >= 0))
    ts = F.col("tpep_pickup_datetime")
    trips = (trips.withColumn("pickup_date", F.date_format(ts, "yyyy-MM-dd"))
                  .withColumn("pickup_hour", F.hour(ts).cast("long"))
                  .withColumn("pickup_dow", F.dayofweek(ts).cast("long")))   # 1=Sunday .. 7=Saturday

    # 3. Transformation: join with the zone table (pickup + dropoff), then the Python UDF.
    #    The zone table is tiny, so it is broadcast: a copy goes to every worker and the
    #    big trip table never has to be shuffled across the network.
    pu = zones.select(F.col("LocationID").alias("PULocationID"),
                      F.col("Borough").alias("pickup_borough"), F.col("Zone").alias("pickup_zone"))
    do = zones.select(F.col("LocationID").alias("DOLocationID"),
                      F.col("Borough").alias("dropoff_borough"), F.col("Zone").alias("dropoff_zone"))
    trips = trips.join(F.broadcast(pu), "PULocationID").join(F.broadcast(do), "DOLocationID")
    trips = trips.withColumn("avg_speed_mph", speed_udf("trip_distance", "duration_sec"))
    trips = trips.filter(F.col("avg_speed_mph").isNotNull()).select(OUTPUT)

    # 4. Export the cleaned trips
    trips.write.mode("overwrite").parquet(trips_path)

    # Average speed per pickup hour, computed from the exported trips. Summing speed*10000
    # as whole numbers makes the result exact, so Spark and Ray get the same value.
    hourly = (spark.read.parquet(trips_path).groupBy("pickup_hour")
              .agg(F.count("*").alias("trip_count"),
                   F.sum(F.round(F.col("avg_speed_mph") * 10000).cast("long")).alias("s"))
              .select("pickup_hour", "trip_count",
                      (F.col("s") / F.col("trip_count") / 10000).alias("avg_speed_mph")))
    hourly.write.mode("overwrite").parquet(hourly_path)
    total = round(time.perf_counter() - start, 2)
    rows = spark.read.parquet(trips_path).count()
    print(f"Spark: {len(files)} files, {rows} rows written, total time {total} s")

    # UDF overhead: same formula as a built-in Spark expression (runs in the JVM)
    # vs. the Python UDF (rows are sent from the JVM to Python and back)
    base = spark.read.parquet(trips_path).select("trip_distance", "duration_sec").cache()
    base.count()
    native = F.round(F.col("trip_distance") / (F.col("duration_sec") / 3600.0), 4)
    native_s = timed(lambda: base.agg(F.sum(native)).collect())
    python_s = timed(lambda: base.agg(F.sum(speed_udf("trip_distance", "duration_sec"))).collect())
    print(f"Spark UDF: built-in {native_s} s, Python UDF {python_s} s")

    os.makedirs(a.results, exist_ok=True)
    path = os.path.join(a.results, "runs.csv")
    new = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["framework", "time", "files", "rows", "total_s", "udf_native_s", "udf_python_s"])
        w.writerow(["spark", time.strftime("%Y-%m-%d %H:%M"), len(files), rows, total, native_s, python_s])
    spark.stop()


if __name__ == "__main__":
    main()
