from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

from src.risk.scoring import score_transactions


def test_risk_scoring_uses_expected_boundaries_and_reasons():
    spark = SparkSession.builder.master("local[1]").appName("risk_scoring_test").getOrCreate()
    schema = StructType(
        [
            StructField("transaction_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("customer_avg_transaction_amount", DoubleType(), True),
            StructField("transactions_last_30min", LongType(), True),
            StructField("new_device_flag", LongType(), True),
            StructField("new_beneficiary_flag", LongType(), True),
            StructField("unusual_hour_flag", LongType(), True),
            StructField("location_change_flag", LongType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("is_fraud", IntegerType(), True),
        ]
    )
    rows = [
        {
            "transaction_id": "TXN-001",
            "customer_id": "CUST-0000001",
            "amount": 950.0,
            "customer_avg_transaction_amount": 100.0,
            "transactions_last_30min": 8,
            "new_device_flag": 1,
            "new_beneficiary_flag": 1,
            "unusual_hour_flag": 1,
            "location_change_flag": 1,
            "transaction_type": "WIRE",
            "is_fraud": 1,
        },
        {
            "transaction_id": "TXN-002",
            "customer_id": "CUST-0000002",
            "amount": 120.0,
            "customer_avg_transaction_amount": 130.0,
            "transactions_last_30min": 1,
            "new_device_flag": 0,
            "new_beneficiary_flag": 0,
            "unusual_hour_flag": 0,
            "location_change_flag": 0,
            "transaction_type": "POS",
            "is_fraud": 0,
        },
    ]
    df = spark.createDataFrame(rows, schema=schema)
    result = score_transactions(df)

    first_row = result.filter("transaction_id = 'TXN-001'").first()
    second_row = result.filter("transaction_id = 'TXN-002'").first()

    assert 0 <= first_row["fraud_risk_score"] <= 100
    assert 0 <= second_row["fraud_risk_score"] <= 100
    assert first_row["risk_level"] in {"HIGH", "CRITICAL"}
    assert second_row["risk_level"] == "LOW"
    assert any("AMOUNT_ANOMALY" in reason for reason in first_row["risk_reasons"])
    spark.stop()
