import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


ROOT_DIR = Path(__file__).resolve().parents[2]


def get_env(name: str, default: str | None = None) -> str | None:
    return os.getenv(name, default)
