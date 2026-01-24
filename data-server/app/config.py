from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_COLOR = os.environ.get("LOG_COLOR", "1") not in {"0", "false", "False"}
BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET", "")
BINANCE_WS_STREAM_BASE = os.environ.get("BINANCE_WS_STREAM_BASE", "")
REST_SLEEP_SEC = float(os.environ.get("REST_SLEEP_SEC", "0.2"))
REST_MAX_RETRIES = int(os.environ.get("REST_MAX_RETRIES", "5"))
REST_BACKOFF_SEC = float(os.environ.get("REST_BACKOFF_SEC", "1.0"))

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "data/market_data.duckdb")
VISION_DOWNLOAD_DIR = os.environ.get("VISION_DOWNLOAD_DIR", "data/vision")
SYMBOLS = [
    symbol.strip().upper()
    for symbol in os.environ.get("SYMBOLS", "BTCUSDT").split(",")
    if symbol.strip()
]
BACKFILL_START_DATE = os.environ.get("BACKFILL_START_DATE", "2024-01-01")


def _parse_backfill_start_ms(value: str) -> int:
    dt = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


BACKFILL_START_MS = _parse_backfill_start_ms(BACKFILL_START_DATE)


def ensure_paths() -> None:
    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    Path(VISION_DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)


ensure_paths()
