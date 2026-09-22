from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from src.common.databricks_client import get_spark_session


def ingest_transactions(source_dir: str, output_path: str) -> None:
    spark = get_spark_session(app_name="FraudLens Bronze Transactions")
    source = Path(source_dir)

    if source.is_dir():
        shard_files = sorted(source.glob("transactions_part_*.parquet"))
        if shard_files:
            df = spark.read.parquet(*[str(path) for path in shard_files])
        else:
            df = spark.read.parquet(str(source))
    else:
        df = spark.read.parquet(str(source))

    df = df.withColumn("_ingested_at", F.current_timestamp())
    df = df.withColumn("_source_file", F.input_file_name())
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(str(output_dir))
    spark.stop()


if __name__ == "__main__":
    from pyspark.sql import functions as F

    parser = argparse.ArgumentParser(description="Ingest transaction bronze data")
    parser.add_argument("--source", default="data/raw")
    parser.add_argument("--output", default="data/bronze/transactions")
    args = parser.parse_args()
    ingest_transactions(args.source, args.output)
