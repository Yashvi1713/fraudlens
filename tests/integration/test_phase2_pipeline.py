from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from databricks.gold.build_gold import build_gold_dataset


def test_gold_pipeline_builds_featured_risk_dataset():
    spark = SparkSession.builder.master("local[1]").appName("phase2_gold_test").getOrCreate()
    schema = StructType(
        [
            StructField("transaction_id", StringType(), False),
            StructField("account_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("merchant_id", StringType(), True),
            StructField("beneficiary_id", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("city", StringType(), True),
            StructField("province", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("is_fraud", LongType(), True),
            StructField("fraud_scenario", StringType(), True),
        ]
    )
    rows = [
        {
            "transaction_id": "TXN-001",
            "account_id": "ACC-00000001",
            "customer_id": "CUST-0000001",
            "timestamp": "2024-01-01T09:00:00",
            "amount": 100.0,
            "transaction_type": "POS",
            "merchant_id": "MER-0000001",
            "beneficiary_id": "BEN-000001",
            "device_id": "DEV-00000001",
            "city": "Toronto",
            "province": "ON",
            "currency": "CAD",
            "is_fraud": 0,
            "fraud_scenario": None,
        },
        {
            "transaction_id": "TXN-002",
            "account_id": "ACC-00000001",
            "customer_id": "CUST-0000001",
            "timestamp": "2024-01-01T09:10:00",
            "amount": 950.0,
            "transaction_type": "WIRE",
            "merchant_id": "MER-0000002",
            "beneficiary_id": "BEN-000002",
            "device_id": "DEV-00000002",
            "city": "Toronto",
            "province": "ON",
            "currency": "CAD",
            "is_fraud": 1,
            "fraud_scenario": "ACCOUNT_TAKEOVER",
        },
    ]
    df = spark.createDataFrame(rows, schema=schema)
    result = build_gold_dataset(df)

    required_columns = {
        "transaction_id",
        "customer_id",
        "fraud_risk_score",
        "risk_level",
        "risk_reasons",
        "is_fraud",
    }
    assert required_columns.issubset(result.columns)
    assert result.count() == 2
    assert result.filter("transaction_id = 'TXN-002'").first()["risk_level"] in {"HIGH", "CRITICAL"}
    spark.stop()
