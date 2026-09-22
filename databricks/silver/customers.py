from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_customers(df: DataFrame) -> DataFrame:
    df = df.filter(F.col("customer_id").isNotNull())
    df = df.dropDuplicates(["customer_id"])
    df = df.withColumn("date_of_birth", F.to_date(F.col("date_of_birth")))
    df = df.withColumn("customer_since", F.to_date(F.col("customer_since")))
    df = df.withColumn("risk_segment", F.upper(F.col("risk_segment")))
    return df
