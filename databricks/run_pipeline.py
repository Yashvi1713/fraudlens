from __future__ import annotations

import argparse
from pathlib import Path

from databricks.gold.build_gold import build_gold_dataset
from databricks.silver.accounts import transform_accounts
from databricks.silver.customers import transform_customers
from databricks.silver.devices import transform_devices
from databricks.silver.transactions import transform_transactions
from src.common.databricks_client import get_spark_session, write_dataframe_to_databricks


def run_local_pipeline(base_dir: str | Path = "data", publish_databricks: bool = False) -> dict[str, int]:
    base = Path(base_dir)
    bronze_dir = base / "bronze"
    silver_dir = base / "silver"
    gold_dir = base / "gold"
    silver_dir.mkdir(parents=True, exist_ok=True)
    gold_dir.mkdir(parents=True, exist_ok=True)

    spark = get_spark_session(app_name="FraudLens Local Medallion Pipeline")

    try:
        transactions_bronze = spark.read.parquet(str(bronze_dir / "transactions"))
        customers_bronze = spark.read.parquet(str(bronze_dir / "customers"))
        accounts_bronze = spark.read.parquet(str(bronze_dir / "accounts"))
        devices_bronze = spark.read.parquet(str(bronze_dir / "devices"))

        transactions_silver = transform_transactions(transactions_bronze)
        customers_silver = transform_customers(customers_bronze)
        accounts_silver = transform_accounts(accounts_bronze)
        devices_silver = transform_devices(devices_bronze)

        transactions_silver.write.mode("overwrite").parquet(str(silver_dir / "transactions"))
        customers_silver.write.mode("overwrite").parquet(str(silver_dir / "customers"))
        accounts_silver.write.mode("overwrite").parquet(str(silver_dir / "accounts"))
        devices_silver.write.mode("overwrite").parquet(str(silver_dir / "devices"))

        gold_transactions = build_gold_dataset(transactions_silver)
        gold_transactions.write.mode("overwrite").parquet(str(gold_dir / "transactions"))

        if publish_databricks:
            write_dataframe_to_databricks(transactions_silver, "transactions", schema="silver")
            write_dataframe_to_databricks(customers_silver, "customers", schema="silver")
            write_dataframe_to_databricks(accounts_silver, "accounts", schema="silver")
            write_dataframe_to_databricks(devices_silver, "devices", schema="silver")
            write_dataframe_to_databricks(gold_transactions, "transactions", schema="gold")

        return {
            "transactions_bronze": int(transactions_bronze.count()),
            "transactions_silver": int(transactions_silver.count()),
            "customers_silver": int(customers_silver.count()),
            "accounts_silver": int(accounts_silver.count()),
            "devices_silver": int(devices_silver.count()),
            "gold_transactions": int(gold_transactions.count()),
        }
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the FraudLens Bronze -> Silver -> Gold pipeline locally")
    parser.add_argument("--base-dir", default="data", help="Base data directory for bronze/silver/gold outputs")
    parser.add_argument(
        "--publish-databricks",
        action="store_true",
        help="Attempt to write the Silver and Gold outputs to Unity Catalog if DATABRICKS env vars are configured.",
    )
    args = parser.parse_args()
    print(run_local_pipeline(args.base_dir, publish_databricks=args.publish_databricks))
