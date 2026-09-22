from __future__ import annotations

from pathlib import Path

from src.common.databricks_client import ensure_local_directories, get_databricks_config


def setup_local_environment() -> None:
    root = Path(__file__).resolve().parents[2]
    directories = ensure_local_directories(root)
    config = get_databricks_config()
    print(f"Using APP_ENV={config.app_env}")
    print(f"Catalog configured: {config.catalog}")
    print("Local directory readiness:")
    for directory in directories:
        print(f" - {directory.relative_to(root)}")


if __name__ == "__main__":
    setup_local_environment()
