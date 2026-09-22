from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from src.common.databricks_client import get_spark_session


def ingest_accounts(source_path: str, output_path: str) -> None:
    spark = get_spark_session(app_name="FraudLens Bronze Accounts")
    df = spark.read.option("multiline", "false").json(source_path)
    df = df.withColumn("_ingested_at", F.current_timestamp())
    df = df.withColumn("_source_file", F.lit(source_path))
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(str(output_dir))
    spark.stop()


if __name__ == "__main__":
    from pyspark.sql import functions as F

    parser = argparse.ArgumentParser(description="Ingest account bronze data")
    parser.add_argument("--source", default="data/raw/accounts.jsonl")
    parser.add_argument("--output", default="data/bronze/accounts")
    args = parser.parse_args()
    ingest_accounts(args.source, args.output)
