"""
compare_outputs.py - check that Spark and Ray produced exactly the same result.

Usage:
  python3 compare_outputs.py /data/out/spark /data/out/ray
"""
import sys

import numpy as np
import pandas as pd
import pyarrow.dataset as ds


def fingerprint(path):
    """Row count + a hash of all rows that does not depend on row order."""
    rows, total = 0, 0
    for batch in ds.dataset(path, format="parquet").to_batches():
        df = batch.to_pandas(ignore_metadata=True)
        df = df[sorted(df.columns)]
        rows += len(df)
        h = pd.util.hash_pandas_object(df, index=False).to_numpy().sum(dtype=np.uint64)
        total = (total + int(h)) % 2**64
    return rows, total


spark_dir, ray_dir = sys.argv[1], sys.argv[2]

spark_rows, spark_hash = fingerprint(f"{spark_dir}/trips")
ray_rows, ray_hash = fingerprint(f"{ray_dir}/trips")
print(f"rows:  spark {spark_rows}  ray {ray_rows}")
print(f"hash:  spark {spark_hash:x}  ray {ray_hash:x}")



def read_hourly(path):
    # ignore_metadata: Ray's files ask pandas for different (pyarrow) column types; compare plain data
    df = ds.dataset(path, format="parquet").to_table().to_pandas(ignore_metadata=True)
    return df.sort_values("pickup_hour").reset_index(drop=True)


spark_h = read_hourly(f"{spark_dir}/avg_speed_per_hour")
ray_h = read_hourly(f"{ray_dir}/avg_speed_per_hour")
same_hourly = spark_h.equals(ray_h)
print(f"hourly averages identical: {same_hourly}")

if spark_rows == ray_rows and spark_hash == ray_hash and same_hourly:
    print("RESULT: IDENTICAL")
else:
    print("RESULT: DIFFERENT")
    sys.exit(1)
