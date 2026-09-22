from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def score_transactions(df: DataFrame) -> DataFrame:
    """Apply a transparent rules-based fraud risk score to each transaction."""
    df = df.withColumn(
        "amount_ratio",
        F.when(F.col("customer_avg_transaction_amount").isNull() | (F.col("customer_avg_transaction_amount") == 0), 0.0)
        .otherwise(F.col("amount") / F.col("customer_avg_transaction_amount")),
    )

    large_transfer_flag = (F.col("amount") >= 500) & F.col("transaction_type").isin("WIRE", "E_TRANSFER")
    amount_anomaly_flag = (F.col("amount_ratio") > 3.0) | large_transfer_flag

    df = df.withColumn(
        "risk_score",
        (
            F.when(amount_anomaly_flag, 35).otherwise(0)
            + F.when(F.col("transactions_last_30min") > 5, 20).otherwise(0)
            + F.when(F.col("new_device_flag") == 1, 20).otherwise(0)
            + F.when(F.col("new_beneficiary_flag") == 1, 20).otherwise(0)
            + F.when(F.col("unusual_hour_flag") == 1, 10).otherwise(0)
            + F.when(F.col("location_change_flag") == 1, 10).otherwise(0)
            + F.when(F.col("transaction_type").isin("WIRE", "E_TRANSFER"), 10).otherwise(0)
        ),
    )

    df = df.withColumn("fraud_risk_score", F.expr("least(100, greatest(0, risk_score))"))
    df = df.withColumn(
        "risk_level",
        F.when(F.col("fraud_risk_score") <= 29, F.lit("LOW"))
        .when(F.col("fraud_risk_score") <= 59, F.lit("MEDIUM"))
        .when(F.col("fraud_risk_score") <= 79, F.lit("HIGH"))
        .otherwise(F.lit("CRITICAL")),
    )

    reasons = F.array(
        F.when(amount_anomaly_flag, F.lit("AMOUNT_ANOMALY")).otherwise(F.lit(None)),
        F.when(F.col("transactions_last_30min") > 5, F.lit("HIGH_VELOCITY")).otherwise(F.lit(None)),
        F.when(F.col("new_device_flag") == 1, F.lit("NEW_DEVICE")).otherwise(F.lit(None)),
        F.when(F.col("new_beneficiary_flag") == 1, F.lit("NEW_BENEFICIARY")).otherwise(F.lit(None)),
        F.when(F.col("unusual_hour_flag") == 1, F.lit("UNUSUAL_HOUR")).otherwise(F.lit(None)),
        F.when(F.col("location_change_flag") == 1, F.lit("LOCATION_ANOMALY")).otherwise(F.lit(None)),
        F.when(F.col("transaction_type").isin("WIRE", "E_TRANSFER"), F.lit("HIGH_RISK_TRANSFER")).otherwise(F.lit(None)),
    )
    df = df.withColumn("risk_reasons", F.filter(reasons, lambda x: x.isNotNull()))

    select_cols = [
        "transaction_id",
        "amount",
        "transaction_type",
        "fraud_risk_score",
        "risk_level",
        "risk_reasons",
        "is_fraud",
    ]
    if "customer_id" in df.columns:
        select_cols.insert(1, "customer_id")

    return df.select(*select_cols)
