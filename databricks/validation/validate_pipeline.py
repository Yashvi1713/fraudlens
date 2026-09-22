from __future__ import annotations

from pathlib import Path

import pandas as pd


def validate_transaction_files(raw_dir: str | Path) -> dict[str, int | bool]:
    base = Path(raw_dir)
    files = sorted(base.glob("transactions_part_*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {base}")
    df = pd.concat([pd.read_parquet(file) for file in files], ignore_index=True)
    return {
        "row_count": int(len(df)),
        "unique_transaction_ids": int(df["transaction_id"].nunique()),
        "fraud_rows": int(df["is_fraud"].sum()),
        "fraud_scenario_count": int(df.loc[df["is_fraud"] == True, "fraud_scenario"].notna().sum()),
        "files_present": True,
    }


if __name__ == "__main__":
    metrics = validate_transaction_files("data/raw")
    print(metrics)
