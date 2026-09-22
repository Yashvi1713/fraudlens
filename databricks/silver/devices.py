from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_devices(df: DataFrame) -> DataFrame:
    df = df.filter(F.col("device_id").isNotNull())
    df = df.filter(F.col("customer_id").isNotNull())
    df = df.dropDuplicates(["device_id"])
    df = df.withColumn("device_type", F.upper(F.col("device_type")))
    df = df.withColumn("os", F.upper(F.col("os")))
    df = df.withColumn("trusted", F.col("trusted").cast("boolean"))
    df = df.withColumn("first_seen", F.to_date(F.col("first_seen")))
    return df
