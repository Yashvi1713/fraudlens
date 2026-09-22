from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_accounts(df: DataFrame) -> DataFrame:
    df = df.filter(F.col("account_id").isNotNull())
    df = df.filter(F.col("customer_id").isNotNull())
    df = df.dropDuplicates(["account_id"])
    df = df.withColumn("account_type", F.upper(F.col("account_type")))
    df = df.withColumn("balance", F.col("balance").cast("double"))
    df = df.withColumn("opened_date", F.to_date(F.col("opened_date")))
    df = df.withColumn("status", F.upper(F.col("status")))
    return df
