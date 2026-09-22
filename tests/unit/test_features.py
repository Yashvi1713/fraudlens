from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from src.features.fraud_features import compute_fraud_features


def test_feature_calculations_include_expected_columns():
    spark = SparkSession.builder.master("local[1]").appName("fraud_features_test").getOrCreate()
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
            "amount": 500.0,
            "transaction_type": "POS",
            "merchant_id": "MER-0000002",
            "beneficiary_id": "BEN-000002",
            "device_id": "DEV-00000002",
            "city": "Toronto",
            "province": "ON",
            "currency": "CAD",
            "is_fraud": 0,
            "fraud_scenario": None,
        },
        {
            "transaction_id": "TXN-003",
            "account_id": "ACC-00000002",
            "customer_id": "CUST-0000002",
            "timestamp": "2024-01-01T20:00:00",
            "amount": 50.0,
            "transaction_type": "E_TRANSFER",
            "merchant_id": "MER-0000003",
            "beneficiary_id": "BEN-000003",
            "device_id": "DEV-00000003",
            "city": "Ottawa",
            "province": "ON",
            "currency": "CAD",
            "is_fraud": 0,
            "fraud_scenario": None,
        },
    ]
    df = spark.createDataFrame(rows, schema=schema)
    result = compute_fraud_features(df)

    expected_columns = {
        "customer_avg_transaction_amount",
        "customer_std_transaction_amount",
        "amount_vs_customer_avg",
        "transactions_last_30min",
        "transactions_last_24h",
        "amount_last_24h",
        "new_device_flag",
        "new_beneficiary_flag",
        "unusual_hour_flag",
        "location_change_flag",
        "beneficiary_count_24h",
    }

    assert expected_columns.issubset(set(result.columns))
    assert result.filter("transaction_id = 'TXN-001'").first()["transactions_last_30min"] == 1
    assert result.filter("transaction_id = 'TXN-002'").first()["transactions_last_30min"] == 2
    spark.stop()
