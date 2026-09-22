from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_transactions(df: DataFrame) -> DataFrame:
    df = df.filter(F.col("transaction_id").isNotNull())
    df = df.filter(F.col("customer_id").isNotNull())
    df = df.filter(F.col("account_id").isNotNull())
    df = df.filter(F.col("device_id").isNotNull())
    df = df.dropDuplicates(["transaction_id"])
    df = df.withColumn("timestamp", F.to_timestamp(F.col("timestamp")))
    df = df.withColumn("amount", F.col("amount").cast("double"))
    df = df.withColumn("transaction_type", F.upper(F.col("transaction_type")))
    df = df.withColumn("currency", F.upper(F.col("currency")))
    df = df.withColumn("is_fraud", F.coalesce(F.col("is_fraud"), F.lit(False)))
    df = df.withColumn("fraud_scenario", F.when(F.col("is_fraud") == True, F.col("fraud_scenario")).otherwise(F.lit(None)))
    return df
