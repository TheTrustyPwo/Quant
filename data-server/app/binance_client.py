from __future__ import annotations

import io
import json
import logging
import time
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, AsyncGenerator

import httpx
import numpy as np
import pandas as pd
import websockets
from tqdm import tqdm
from binance_common.configuration import ConfigurationRestAPI
from binance_common.constants import (
    DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL,
    DERIVATIVES_TRADING_USDS_FUTURES_WS_API_PROD_URL,
)
from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
    DerivativesTradingUsdsFutures,
)

from app.config import (
    BINANCE_API_KEY,
    BINANCE_API_SECRET,
    BINANCE_WS_STREAM_BASE,
    REST_BACKOFF_SEC,
    REST_MAX_RETRIES,
    REST_SLEEP_SEC,
)
from app.models import AggTrade

logger = logging.getLogger(__name__)

# HTTP client with connection pooling for better performance
_HTTP_CLIENT: httpx.Client | None = None


def _get_http_client() -> httpx.Client:
    """Get or create a shared HTTP client with connection pooling."""
    global _HTTP_CLIENT
    if _HTTP_CLIENT is None:
        _HTTP_CLIENT = httpx.Client(
            timeout=30.0,
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
        )
    return _HTTP_CLIENT

configuration_rest_api = ConfigurationRestAPI(
    api_key=BINANCE_API_KEY,
    api_secret=BINANCE_API_SECRET,
    base_path=DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL,
)
client = DerivativesTradingUsdsFutures(config_rest_api=configuration_rest_api)


def _rest_headers() -> dict[str, str]:
    headers: dict[str, str] = {}
    if BINANCE_API_KEY:
        headers["X-MBX-APIKEY"] = BINANCE_API_KEY
    return headers


def _parse_rest_agg_trade(symbol: str, item: dict[str, Any]) -> AggTrade:
    trade_id = int(item.get("a") or item.get("aggTradeId") or item.get("id"))
    price = float(item.get("p") or item.get("price"))
    qty = float(item.get("q") or item.get("qty"))
    ts_ms = int(item.get("T") or item.get("timestamp"))
    is_buyer_maker = bool(item.get("m") if "m" in item else item.get("isBuyerMaker"))
    return AggTrade(
        symbol=symbol,
        trade_id=trade_id,
        price=price,
        qty=qty,
        ts_ms=ts_ms,
        is_buyer_maker=is_buyer_maker,
    )


def fetch_recent_agg_trades(
    symbol: str,
    start_ms: int,
    end_ms: int,
    show_progress: bool = False,
) -> list[AggTrade]:
    """Fetch aggregated trades from Binance REST API.

    Uses pagination to fetch all trades in the given time range.
    Handles rate limiting with exponential backoff.

    Note: For large datasets, consider using fetch_recent_agg_trades_df() instead
    which returns a DataFrame directly and is much more efficient.
    """
    df = fetch_recent_agg_trades_df(symbol, start_ms, end_ms, show_progress)
    if df.empty:
        return []
    # Convert DataFrame to list of AggTrade objects
    return [
        AggTrade(
            symbol=row.symbol,
            trade_id=int(row.trade_id),
            price=float(row.price),
            qty=float(row.qty),
            ts_ms=int(row.ts_ms),
            is_buyer_maker=bool(row.is_buyer_maker),
        )
        for row in df.itertuples(index=False)
    ]


def fetch_recent_agg_trades_df(
    symbol: str,
    start_ms: int,
    end_ms: int,
    show_progress: bool = False,
) -> pd.DataFrame:
    """Fetch aggregated trades from Binance REST API as a DataFrame.

    This is much more efficient than fetch_recent_agg_trades() for large datasets
    as it avoids creating millions of Python objects.

    Uses pagination to fetch all trades in the given time range.
    Handles rate limiting with exponential backoff.
    """
    symbol_upper = symbol.upper()
    base_url = DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL.rstrip("/")
    url = f"{base_url}/fapi/v1/aggTrades"
    current_start = start_ms
    total_ms = max(end_ms - start_ms + 1, 1)
    client = _get_http_client()

    # Accumulate data directly into lists (much faster than creating objects)
    trade_ids: list[int] = []
    prices: list[float] = []
    qtys: list[float] = []
    ts_ms_list: list[int] = []
    is_buyer_maker_list: list[bool] = []

    progress = tqdm(
        total=total_ms,
        unit="ms",
        desc=f"REST {symbol_upper}",
        disable=not show_progress,
        bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt}",
    )
    try:
        while current_start <= end_ms:
            params = {
                "symbol": symbol_upper,
                "startTime": current_start,
                "endTime": end_ms,
                "limit": 1000,
            }
            data = None
            for attempt in range(REST_MAX_RETRIES + 1):
                try:
                    response = client.get(url, params=params, headers=_rest_headers())
                    if response.status_code == 429:
                        retry_after = response.headers.get("Retry-After")
                        if retry_after and retry_after.isdigit():
                            delay = float(retry_after)
                        else:
                            delay = REST_BACKOFF_SEC * (2 ** attempt)
                        logger.warning(
                            "%s | Rate limited (429), waiting %.1fs",
                            symbol_upper,
                            delay,
                        )
                        time.sleep(delay)
                        continue
                    response.raise_for_status()
                    data = response.json()
                    break
                except httpx.RequestError as e:
                    if attempt < REST_MAX_RETRIES:
                        delay = REST_BACKOFF_SEC * (2 ** attempt)
                        logger.warning(
                            "%s | Request error: %s, retrying in %.1fs",
                            symbol_upper,
                            type(e).__name__,
                            delay,
                        )
                        time.sleep(delay)
                    else:
                        raise

            if data is None:
                raise httpx.HTTPStatusError(
                    "Exceeded REST retries for aggTrades",
                    request=response.request,
                    response=response,
                )
            if not data:
                break

            # Accumulate directly into lists (no object creation)
            for item in data:
                trade_ids.append(int(item.get("a") or item.get("aggTradeId") or item.get("id")))
                prices.append(float(item.get("p") or item.get("price")))
                qtys.append(float(item.get("q") or item.get("qty")))
                ts_ms_list.append(int(item.get("T") or item.get("timestamp")))
                is_buyer_maker_list.append(bool(item.get("m") if "m" in item else item.get("isBuyerMaker")))

            last_ts = data[-1].get("T") or data[-1].get("timestamp")
            if last_ts is None:
                break
            last_ts_int = int(last_ts)
            progress.update(max(0, min(end_ms, last_ts_int) - current_start + 1))
            current_start = last_ts_int + 1
            if len(data) < 1000:
                break
            if REST_SLEEP_SEC > 0:
                time.sleep(REST_SLEEP_SEC)
    finally:
        if not progress.disable and progress.n < total_ms:
            progress.update(total_ms - progress.n)
        progress.close()

    if not trade_ids:
        return pd.DataFrame(columns=["symbol", "trade_id", "price", "qty", "ts_ms", "is_buyer_maker"])

    # Build DataFrame directly from lists (very fast, no Python object overhead)
    return pd.DataFrame({
        "symbol": symbol_upper,
        "trade_id": np.array(trade_ids, dtype=np.int64),
        "price": np.array(prices, dtype=np.float64),
        "qty": np.array(qtys, dtype=np.float64),
        "ts_ms": np.array(ts_ms_list, dtype=np.int64),
        "is_buyer_maker": np.array(is_buyer_maker_list, dtype=np.bool_),
    })


def _build_ws_url(stream: str) -> str:
    base = (
        BINANCE_WS_STREAM_BASE
        or DERIVATIVES_TRADING_USDS_FUTURES_WS_API_PROD_URL
    ).rstrip("/")
    if "ws-fapi" in base:
        logger.warning(
            "WS API base does not support market data streams, using fstream binance ws"
        )
        base = "wss://fstream.binance.com/ws"
    if base.endswith("/stream"):
        return f"{base}?streams={stream}"
    if base.endswith("/ws"):
        return f"{base}/{stream}"
    return f"{base}/ws/{stream}"


async def stream_agg_trades(symbol: str) -> AsyncGenerator[AggTrade, None]:
    """Stream aggregated trades from Binance WebSocket.

    Yields AggTrade objects as they arrive from the WebSocket stream.
    The connection uses ping/pong to detect disconnections.
    """
    stream = f"{symbol.lower()}@aggTrade"
    url = _build_ws_url(stream)
    logger.debug("%s | WebSocket connecting to %s", symbol.upper(), url)

    async with websockets.connect(
        url,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=5,
    ) as websocket:
        logger.debug("%s | WebSocket connected", symbol.upper())
        while True:
            message = await websocket.recv()
            payload = json.loads(message)
            data = payload.get("data", payload)

            # Skip non-trade messages
            if data.get("e") != "aggTrade":
                continue

            yield AggTrade(
                symbol=data.get("s", symbol.upper()),
                trade_id=int(data["a"]),
                price=float(data["p"]),
                qty=float(data["q"]),
                ts_ms=int(data["T"]),
                is_buyer_maker=bool(data["m"]),
            )


def _download_file(url: str, dest_path: Path) -> Path:
    """Download a file from URL to destination path.

    Uses caching - if the file already exists and has content, it will be reused.
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    if dest_path.exists() and dest_path.stat().st_size > 0:
        logger.debug("Using cached: %s", dest_path.name)
        return dest_path

    logger.debug("Downloading: %s", dest_path.name)
    with httpx.stream("GET", url, timeout=120, follow_redirects=True) as response:
        response.raise_for_status()
        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0

        with dest_path.open("wb") as handle:
            for chunk in response.iter_bytes(chunk_size=65536):
                handle.write(chunk)
                downloaded += len(chunk)

        if total_size > 0:
            size_mb = total_size / (1024 * 1024)
            logger.debug("Downloaded: %s (%.1f MB)", dest_path.name, size_mb)

    return dest_path


def download_vision_aggtrades_month(symbol: str, year: int, month: int, dest_path: Path) -> Path:
    symbol = symbol.upper()
    url = (
        "https://data.binance.vision/data/futures/um/monthly/aggTrades/"
        f"{symbol}/{symbol}-aggTrades-{year}-{month:02d}.zip"
    )
    return _download_file(url, dest_path)


def download_vision_aggtrades_day(symbol: str, year: int, month: int, day: int, dest_path: Path) -> Path:
    symbol = symbol.upper()
    url = (
        "https://data.binance.vision/data/futures/um/daily/aggTrades/"
        f"{symbol}/{symbol}-aggTrades-{year}-{month:02d}-{day:02d}.zip"
    )
    return _download_file(url, dest_path)


# ============================================================================
# Optimized Vision ZIP loading with numpy acceleration
# ============================================================================

# Column name mappings for Vision CSV files (computed once)
_VISION_RENAME_MAP = {
    "agg_trade_id": "trade_id",
    "aggTradeId": "trade_id",
    "quantity": "qty",
    "qty": "qty",
    "transact_time": "ts_ms",
    "timestamp": "ts_ms",
    "is_buyer_maker": "is_buyer_maker",
    "isBuyerMaker": "is_buyer_maker",
}

# Optimal dtypes for Vision CSV (avoids type inference overhead)
_VISION_DTYPES = {
    "agg_trade_id": "int64",
    "aggTradeId": "int64",
    "price": "float64",
    "quantity": "float64",
    "qty": "float64",
    "transact_time": "int64",
    "timestamp": "int64",
    "first_trade_id": "int64",  # ignored but may be present
    "last_trade_id": "int64",   # ignored but may be present
    "is_buyer_maker": "object",  # will convert with numpy
    "isBuyerMaker": "object",
}

# Required output columns
_VISION_REQUIRED = {"trade_id", "price", "qty", "ts_ms", "is_buyer_maker"}
_VISION_OUTPUT_COLS = ["symbol", "trade_id", "price", "qty", "ts_ms", "is_buyer_maker"]

# Check if pyarrow is available for faster CSV parsing
try:
    import pyarrow.csv as pa_csv
    _HAS_PYARROW = True
except ImportError:
    _HAS_PYARROW = False


def _convert_is_buyer_maker_numpy(series: pd.Series) -> np.ndarray:
    """
    Convert is_buyer_maker column to boolean using numpy (faster than pandas.isin).

    Handles: True, False, "true", "True", "false", "False", 1, 0, "1", "0"
    """
    arr = series.values

    # If already boolean, return as-is
    if arr.dtype == np.bool_:
        return arr

    # If numeric, convert directly
    if np.issubdtype(arr.dtype, np.number):
        return arr.astype(np.bool_)

    # String/object dtype - use numpy vectorized comparison
    # Convert to lowercase string for comparison
    str_arr = np.char.lower(arr.astype(str))
    return (str_arr == 'true') | (str_arr == '1')


def _filter_by_timestamp_numpy(
    df: pd.DataFrame,
    start_ms: int | None,
    end_ms: int | None,
) -> pd.DataFrame:
    """
    Filter DataFrame by timestamp range using numpy (faster than pandas boolean indexing).
    """
    if start_ms is None and end_ms is None:
        return df

    ts_arr = df["ts_ms"].values

    if start_ms is not None and end_ms is not None:
        mask = (ts_arr >= start_ms) & (ts_arr <= end_ms)
    elif start_ms is not None:
        mask = ts_arr >= start_ms
    else:
        mask = ts_arr <= end_ms

    # Use numpy boolean indexing which is faster for large arrays
    if mask.sum() == len(df):
        return df
    if mask.sum() == 0:
        return df.iloc[:0]

    return df.iloc[mask]


def _read_csv_fast(
    file_handle,
    chunksize: int | None = None,
    use_pyarrow: bool = True,
) -> pd.DataFrame | pd.io.parsers.TextFileReader:
    """
    Read CSV with optimized settings.

    Uses PyArrow backend if available (2-3x faster), otherwise optimized pandas.
    """
    # Common pandas options for speed
    pandas_opts = {
        "dtype": _VISION_DTYPES,
        "na_filter": False,  # No NA handling needed, faster
        "low_memory": False,  # Avoid mixed type warnings
    }

    if chunksize is not None:
        pandas_opts["chunksize"] = chunksize

    # Try PyArrow backend (much faster for large files)
    if use_pyarrow and _HAS_PYARROW and chunksize is None:
        try:
            # PyArrow is only for full file reads, not chunked
            text_stream = io.TextIOWrapper(file_handle, encoding='utf-8')
            return pd.read_csv(
                text_stream,
                engine='pyarrow',
                dtype_backend='numpy_nullable',
            )
        except Exception:
            # Fall back to default engine
            pass

    # Use standard pandas with optimized settings
    text_stream = io.TextIOWrapper(file_handle, encoding='utf-8', newline='')
    return pd.read_csv(text_stream, **pandas_opts)


def _process_vision_chunk(chunk: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """
    Process a chunk of Vision data: rename columns, convert types, select columns.

    Optimized with numpy operations.
    """
    # Rename columns (in-place for speed)
    chunk.rename(columns=_VISION_RENAME_MAP, inplace=True)

    # Validate required columns
    missing = _VISION_REQUIRED - set(chunk.columns)
    if missing:
        raise ValueError(f"Vision CSV missing columns: {sorted(missing)}")

    # Convert is_buyer_maker using numpy (much faster than pandas.isin)
    chunk["is_buyer_maker"] = _convert_is_buyer_maker_numpy(chunk["is_buyer_maker"])

    # Add symbol column
    chunk["symbol"] = symbol

    # Select and reorder columns
    return chunk[_VISION_OUTPUT_COLS]


def load_vision_aggtrades_df(
    zip_path: Path,
    symbol: str,
    start_ms: int | None = None,
    end_ms: int | None = None,
) -> pd.DataFrame:
    """
    Load Vision aggTrades from a zip file into a DataFrame.

    Optimized with:
    - PyArrow CSV backend (if available)
    - Explicit dtypes (no type inference)
    - NumPy-based filtering and type conversion
    """
    symbol = symbol.upper()
    logger.debug("Loading Vision zip: %s", zip_path.name)

    with zipfile.ZipFile(zip_path) as archive:
        names = archive.namelist()
        if not names:
            return pd.DataFrame(columns=_VISION_OUTPUT_COLS)

        with archive.open(names[0]) as file_handle:
            df = _read_csv_fast(file_handle, chunksize=None, use_pyarrow=True)

    # Rename and validate columns
    df.rename(columns=_VISION_RENAME_MAP, inplace=True)
    missing = _VISION_REQUIRED - set(df.columns)
    if missing:
        raise ValueError(f"Vision CSV missing columns: {sorted(missing)}")

    # Filter by timestamp (numpy-accelerated)
    df = _filter_by_timestamp_numpy(df, start_ms, end_ms)

    if df.empty:
        return pd.DataFrame(columns=_VISION_OUTPUT_COLS)

    # Convert is_buyer_maker using numpy
    df["is_buyer_maker"] = _convert_is_buyer_maker_numpy(df["is_buyer_maker"])

    # Add symbol and select columns
    df["symbol"] = symbol
    return df[_VISION_OUTPUT_COLS]


def iter_vision_aggtrades_df(
    zip_path: Path,
    symbol: str,
    start_ms: int | None = None,
    end_ms: int | None = None,
    chunksize: int = 500_000,
):
    """
    Stream Vision aggTrades from a zip file in chunks.

    Optimized with:
    - Explicit dtypes (no type inference per chunk)
    - NumPy-based filtering and type conversion
    - Larger default chunksize for better throughput

    Yields:
        pd.DataFrame chunks with columns: symbol, trade_id, price, qty, ts_ms, is_buyer_maker
    """
    symbol = symbol.upper()
    logger.debug("Streaming Vision zip: %s (chunksize=%d)", zip_path.name, chunksize)

    with zipfile.ZipFile(zip_path) as archive:
        names = archive.namelist()
        if not names:
            return

        with archive.open(names[0]) as file_handle:
            # Use chunked reader with optimized settings
            reader = _read_csv_fast(file_handle, chunksize=chunksize, use_pyarrow=False)

            for chunk in reader:
                # Rename columns (once per chunk, in-place)
                chunk.rename(columns=_VISION_RENAME_MAP, inplace=True)

                # Validate (only on first chunk would be ideal, but keep for safety)
                missing = _VISION_REQUIRED - set(chunk.columns)
                if missing:
                    raise ValueError(f"Vision CSV missing columns: {sorted(missing)}")

                # Filter by timestamp using numpy (faster than pandas)
                if start_ms is not None or end_ms is not None:
                    chunk = _filter_by_timestamp_numpy(chunk, start_ms, end_ms)
                    if chunk.empty:
                        continue

                # Convert is_buyer_maker using numpy
                chunk["is_buyer_maker"] = _convert_is_buyer_maker_numpy(chunk["is_buyer_maker"])

                # Add symbol and yield selected columns
                chunk["symbol"] = symbol
                yield chunk[_VISION_OUTPUT_COLS]


def vision_day_available(date_value: date, now: datetime | None = None) -> bool:
    now = now or datetime.utcnow()
    today = now.date()
    yesterday = today - timedelta(days=1)
    if date_value < yesterday:
        return True
    if date_value == yesterday and now.hour >= 8:
        return True
    return False
