from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.features.fraud_features import compute_fraud_features
from src.risk.scoring import score_transactions


def build_gold_dataset(silver_transactions: DataFrame) -> DataFrame:
    """Create a Gold risk dataset from silver transaction data."""
    required_columns = {
        "transaction_id",
        "customer_id",
        "account_id",
        "amount",
        "transaction_type",
        "device_id",
        "beneficiary_id",
        "merchant_id",
        "city",
        "timestamp",
    }
    missing = sorted(required_columns - set(silver_transactions.columns))
    if missing:
        raise ValueError(f"Missing required columns for Gold build: {missing}")

    enriched = compute_fraud_features(silver_transactions)
    scored = score_transactions(enriched)

    desired_columns = [
        "transaction_id",
        "customer_id",
        "account_id",
        "amount",
        "transaction_type",
        "merchant_id",
        "beneficiary_id",
        "device_id",
        "city",
        "province",
        "currency",
        "timestamp",
        "is_fraud",
        "fraud_scenario",
        "fraud_risk_score",
        "risk_level",
        "risk_reasons",
    ]
    return scored.select([col for col in desired_columns if col in scored.columns])


def write_gold_dataset(df: DataFrame, output_path: str) -> None:
    """Persist the Gold dataset to parquet."""
    df.write.mode("overwrite").parquet(output_path)
