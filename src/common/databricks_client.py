from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class DatabricksConfig:
    host: str | None = None
    http_path: str | None = None
    cluster_id: str | None = None
    token: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    catalog: str = "fraudlens"
    warehouse_id: str | None = None
    app_env: str = "local"

    @classmethod
    def from_env(cls) -> "DatabricksConfig":
        return cls(
            host=os.getenv("DATABRICKS_HOST"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            cluster_id=os.getenv("DATABRICKS_CLUSTER_ID"),
            token=os.getenv("DATABRICKS_TOKEN"),
            client_id=os.getenv("DATABRICKS_CLIENT_ID"),
            client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),
            catalog=os.getenv("DATABRICKS_CATALOG", "fraudlens"),
            warehouse_id=os.getenv("DATABRICKS_WAREHOUSE_ID"),
            app_env=os.getenv("APP_ENV", "local"),
        )

    @property
    def is_configured(self) -> bool:
        return bool(
            self.host
            and self.http_path
            and (self.token or (self.client_id and self.client_secret))
        )

    @property
    def supports_remote_spark_write(self) -> bool:
        return bool(self.is_configured and self.cluster_id)


def get_databricks_config() -> DatabricksConfig:
    return DatabricksConfig.from_env()


def get_databricks_spark_session(app_name: str = "FraudLens AI") -> Any:
    config = get_databricks_config()
    if not config.is_configured:
        raise ValueError(
            "Remote Databricks writes require DATABRICKS_HOST, DATABRICKS_HTTP_PATH, and either DATABRICKS_TOKEN or OAuth DATABRICKS_CLIENT_ID/DATABRICKS_CLIENT_SECRET."
        )
    if not config.cluster_id:
        raise RuntimeError(
            "Databricks Community Edition does not expose a cluster for Databricks Connect remote Spark writes. "
            "Use the local lakehouse pipeline or upgrade to a paid Databricks workspace with cluster support."
        )

    try:
        from databricks.connect import DatabricksSession
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Install databricks-connect to enable Databricks remote Spark sessions for Unity Catalog writes."
        ) from exc

    token = get_access_token(config)
    if not token:
        raise RuntimeError("Databricks access token could not be generated from the configured credentials.")

    return DatabricksSession.builder.remote(
        host=config.host,
        token=token,
        cluster_id=config.cluster_id,
    ).getOrCreate()


def get_access_token(config: DatabricksConfig | None = None) -> str | None:
    cfg = config or get_databricks_config()
    if cfg.token:
        return cfg.token

    if not cfg.client_id or not cfg.client_secret:
        return None

    if not cfg.host:
        return None

    token_url = f"{cfg.host.rstrip('/')}/oidc/v1/token"
    response = requests.post(
        token_url,
        data={"grant_type": "client_credentials", "scope": "all-apis"},
        auth=(cfg.client_id, cfg.client_secret),
        timeout=30,
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"Databricks OAuth token exchange failed: {response.status_code} {response.text}"
        )

    payload = response.json()
    access_token = payload.get("access_token")
    if not access_token:
        raise RuntimeError(f"Databricks OAuth response did not include an access_token: {payload}")
    return access_token


def is_databricks_configured() -> bool:
    return get_databricks_config().is_configured


def write_dataframe_to_databricks(
    df: Any,
    table_name: str,
    *,
    catalog: str | None = None,
    schema: str = "bronze",
    mode: str = "overwrite",
) -> bool:
    config = get_databricks_config()
    if not config.is_configured:
        raise ValueError(
            "Databricks publish requires DATABRICKS_HOST, DATABRICKS_HTTP_PATH, and either DATABRICKS_TOKEN or OAuth DATABRICKS_CLIENT_ID/DATABRICKS_CLIENT_SECRET in the environment."
        )
    if not config.cluster_id:
        raise RuntimeError(
            "Databricks Community Edition does not expose a cluster for Databricks Connect remote Spark writes. "
            "Use the local lakehouse pipeline or move to a paid Databricks workspace with cluster support."
        )

    catalog_name = catalog or config.catalog
    full_table_name = f"{catalog_name}.{schema}.{table_name}"

    spark = get_databricks_spark_session(app_name=f"FraudLens Databricks {table_name}")
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema}")
        df.write.format("delta").mode(mode).saveAsTable(full_table_name)
        return True
    finally:
        spark.stop()


def get_spark_session(app_name: str = "FraudLens AI", catalog: str | None = None) -> Any:
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    builder = builder.config("spark.sql.shuffle.partitions", "8")
    builder = builder.config("spark.sql.adaptive.enabled", "true")
    if catalog:
        builder = builder.config("spark.sql.catalogImplementation", "hive")

    spark = builder.getOrCreate()
    return spark


def ensure_local_directories(root_dir: str | Path | None = None) -> list[Path]:
    base = Path(root_dir) if root_dir else Path(__file__).resolve().parents[2]
    directories = [
        base / "data" / "raw",
        base / "data" / "sample",
        base / "data" / "bronze",
        base / "data" / "silver",
        base / "data" / "gold",
    ]
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
    return directories
