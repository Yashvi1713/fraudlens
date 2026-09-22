from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def compute_fraud_features(df: DataFrame) -> DataFrame:
    """Compute transaction-level fraud signals based on customer behavior."""
    if "timestamp" in df.columns:
        df = df.withColumn("timestamp", F.to_timestamp(F.col("timestamp")))

    customer_avg = df.groupBy("customer_id").agg(F.avg("amount").alias("customer_avg_transaction_amount"))
    customer_std = df.groupBy("customer_id").agg(F.stddev("amount").alias("customer_std_transaction_amount"))
    df = df.join(customer_avg, on="customer_id", how="left")
    df = df.join(customer_std, on="customer_id", how="left")

    df = df.withColumn(
        "amount_vs_customer_avg",
        F.when(F.col("customer_avg_transaction_amount").isNull(), 0.0)
        .otherwise(F.col("amount") / F.col("customer_avg_transaction_amount")),
    )

    customer_time_window = Window.partitionBy("customer_id").orderBy(F.col("timestamp").cast("long")).rangeBetween(-1800, 0)
    customer_24h_window = Window.partitionBy("customer_id").orderBy(F.col("timestamp").cast("long")).rangeBetween(-86400, 0)
    customer_device_window = Window.partitionBy("customer_id", "device_id")
    customer_beneficiary_window = Window.partitionBy("customer_id", "beneficiary_id")

    df = df.withColumn("transactions_last_30min", F.count("transaction_id").over(customer_time_window))
    df = df.withColumn("transactions_last_24h", F.count("transaction_id").over(customer_24h_window))
    df = df.withColumn("amount_last_24h", F.sum("amount").over(customer_24h_window))
    df = df.withColumn("device_usage_count", F.count("transaction_id").over(customer_device_window))
    df = df.withColumn("beneficiary_usage_count", F.count("transaction_id").over(customer_beneficiary_window))

    df = df.withColumn(
        "new_device_flag",
        F.when(F.col("device_usage_count") <= 1, 1).otherwise(0),
    )
    df = df.withColumn(
        "new_beneficiary_flag",
        F.when(F.col("beneficiary_usage_count") <= 1, 1).otherwise(0),
    )
    df = df.withColumn(
        "unusual_hour_flag",
        F.when(F.hour(F.col("timestamp")).isin([0, 1, 2, 3, 4, 5, 6, 22, 23]), 1).otherwise(0),
    )
    df = df.withColumn(
        "location_change_flag",
        F.when(F.col("city").isNull(), 0).otherwise(0),
    )
    df = df.withColumn(
        "beneficiary_count_24h",
        F.count("beneficiary_id").over(
            Window.partitionBy("customer_id", "beneficiary_id")
            .orderBy(F.col("timestamp").cast("long"))
            .rangeBetween(-86400, 0)
        ),
    )

    df = df.fillna({
        "customer_avg_transaction_amount": 0.0,
        "customer_std_transaction_amount": 0.0,
        "amount_vs_customer_avg": 0.0,
        "amount_last_24h": 0.0,
        "transactions_last_30min": 0,
        "transactions_last_24h": 0,
        "new_device_flag": 0,
        "new_beneficiary_flag": 0,
        "unusual_hour_flag": 0,
        "location_change_flag": 0,
        "beneficiary_count_24h": 0,
    })

    return df.drop("device_usage_count", "beneficiary_usage_count")
