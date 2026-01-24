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
    """
    trades: list[AggTrade] = []
    symbol_upper = symbol.upper()
    base_url = DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL.rstrip("/")
    url = f"{base_url}/fapi/v1/aggTrades"
    current_start = start_ms
    total_ms = max(end_ms - start_ms + 1, 1)
    client = _get_http_client()

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
            for item in data:
                trades.append(_parse_rest_agg_trade(symbol_upper, item))
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
    return trades


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


def load_vision_aggtrades_df(
    zip_path: Path,
    symbol: str,
    start_ms: int | None = None,
    end_ms: int | None = None,
) -> pd.DataFrame:
    symbol = symbol.upper()
    logger.info("Loading Vision aggTrades zip: %s", zip_path)
    with zipfile.ZipFile(zip_path) as archive:
        names = archive.namelist()
        if not names:
            return pd.DataFrame(
                columns=["symbol", "trade_id", "price", "qty", "ts_ms", "is_buyer_maker"]
            )
        with archive.open(names[0]) as file_handle:
            text_stream = io.TextIOWrapper(file_handle)
            df = pd.read_csv(text_stream)

    rename_map = {
        "agg_trade_id": "trade_id",
        "aggTradeId": "trade_id",
        "quantity": "qty",
        "qty": "qty",
        "transact_time": "ts_ms",
        "timestamp": "ts_ms",
        "is_buyer_maker": "is_buyer_maker",
        "isBuyerMaker": "is_buyer_maker",
    }
    df = df.rename(columns=rename_map)
    required = {"trade_id", "price", "qty", "ts_ms", "is_buyer_maker"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Vision CSV missing columns: {sorted(missing)}")

    if start_ms is not None:
        df = df[df["ts_ms"] >= start_ms]
    if end_ms is not None:
        df = df[df["ts_ms"] <= end_ms]

    if df.empty:
        return pd.DataFrame(
            columns=["symbol", "trade_id", "price", "qty", "ts_ms", "is_buyer_maker"]
        )

    df["symbol"] = symbol
    if df["is_buyer_maker"].dtype == object:
        df["is_buyer_maker"] = df["is_buyer_maker"].isin(["true", "True", True, 1, "1"])
    df = df[["symbol", "trade_id", "price", "qty", "ts_ms", "is_buyer_maker"]]
    return df


def iter_vision_aggtrades_df(
    zip_path: Path,
    symbol: str,
    start_ms: int | None = None,
    end_ms: int | None = None,
    chunksize: int = 500_000,
):
    symbol = symbol.upper()
    logger.info("Streaming Vision aggTrades zip: %s", zip_path)
    with zipfile.ZipFile(zip_path) as archive:
        names = archive.namelist()
        if not names:
            return
        with archive.open(names[0]) as file_handle:
            text_stream = io.TextIOWrapper(file_handle)
            for chunk in pd.read_csv(text_stream, chunksize=chunksize):
                rename_map = {
                    "agg_trade_id": "trade_id",
                    "aggTradeId": "trade_id",
                    "quantity": "qty",
                    "qty": "qty",
                    "transact_time": "ts_ms",
                    "timestamp": "ts_ms",
                    "is_buyer_maker": "is_buyer_maker",
                    "isBuyerMaker": "is_buyer_maker",
                }
                chunk = chunk.rename(columns=rename_map)
                required = {"trade_id", "price", "qty", "ts_ms", "is_buyer_maker"}
                missing = required - set(chunk.columns)
                if missing:
                    raise ValueError(f"Vision CSV missing columns: {sorted(missing)}")
                if start_ms is not None:
                    chunk = chunk[chunk["ts_ms"] >= start_ms]
                if end_ms is not None:
                    chunk = chunk[chunk["ts_ms"] <= end_ms]
                if chunk.empty:
                    continue
                chunk["symbol"] = symbol
                if chunk["is_buyer_maker"].dtype == object:
                    chunk["is_buyer_maker"] = chunk["is_buyer_maker"].isin(
                        ["true", "True", True, 1, "1"]
                    )
                yield chunk[
                    ["symbol", "trade_id", "price", "qty", "ts_ms", "is_buyer_maker"]
                ]


def vision_day_available(date_value: date, now: datetime | None = None) -> bool:
    now = now or datetime.utcnow()
    today = now.date()
    yesterday = today - timedelta(days=1)
    if date_value < yesterday:
        return True
    if date_value == yesterday and now.hour >= 8:
        return True
    return False
