from __future__ import annotations

import asyncio
import logging
import sys
import time
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app.backfill import BackfillManager
from app.config import BACKFILL_START_MS, LOG_COLOR, LOG_LEVEL, SYMBOLS, ensure_paths
from app.db import init_db
from app.ingest import run_live_ingest_with_backfill
from app.models import Candle, Coverage, CvdCandle, VolumeProfileBucket
from app import pubsub
from app.services import MarketDataService
from app.utils import parse_interval_to_ms


class PrettyFormatter(logging.Formatter):
    """Enhanced logging formatter with colors, icons, and component tags."""

    # ANSI color codes
    RESET = "\x1b[0m"
    BOLD = "\x1b[1m"
    DIM = "\x1b[2m"

    # Colors
    COLORS = {
        logging.DEBUG: "\x1b[38;5;243m",     # Gray
        logging.INFO: "\x1b[38;5;75m",       # Light blue
        logging.WARNING: "\x1b[38;5;214m",   # Orange
        logging.ERROR: "\x1b[38;5;196m",     # Red
        logging.CRITICAL: "\x1b[38;5;199m",  # Magenta
    }

    # Level icons
    ICONS = {
        logging.DEBUG: "...",
        logging.INFO: "-->",
        logging.WARNING: "/!\\",
        logging.ERROR: "[X]",
        logging.CRITICAL: "!!!",
    }

    # Component colors for different modules
    COMPONENT_COLORS = {
        "app.db": "\x1b[38;5;141m",          # Purple
        "app.ingest": "\x1b[38;5;114m",      # Green
        "app.backfill": "\x1b[38;5;215m",    # Orange
        "app.binance_client": "\x1b[38;5;81m",  # Cyan
        "app.main": "\x1b[38;5;75m",         # Blue
        "app.pubsub": "\x1b[38;5;219m",      # Pink
        "app.services": "\x1b[38;5;228m",    # Yellow
        "uvicorn": "\x1b[38;5;248m",         # Gray
        "uvicorn.error": "\x1b[38;5;248m",
        "uvicorn.access": "\x1b[38;5;248m",
    }

    # Short component names
    COMPONENT_NAMES = {
        "app.db": "DB",
        "app.ingest": "INGEST",
        "app.backfill": "BACKFILL",
        "app.binance_client": "BINANCE",
        "app.main": "SERVER",
        "app.pubsub": "PUBSUB",
        "app.services": "SERVICE",
        "uvicorn": "UVICORN",
        "uvicorn.error": "UVICORN",
        "uvicorn.access": "HTTP",
    }

    def __init__(self, use_color: bool = True) -> None:
        super().__init__()
        self._use_color = use_color

    def _get_component(self, name: str) -> tuple[str, str]:
        """Get component name and color from logger name."""
        for key in self.COMPONENT_NAMES:
            if name.startswith(key):
                return self.COMPONENT_NAMES[key], self.COMPONENT_COLORS.get(key, "")
        return name.split(".")[-1].upper()[:8], ""

    def format(self, record: logging.LogRecord) -> str:
        # Time formatting
        dt = datetime.fromtimestamp(record.created)
        time_str = dt.strftime("%H:%M:%S")
        ms_str = f".{int(record.msecs):03d}"

        # Component
        component, comp_color = self._get_component(record.name)

        # Level
        level_color = self.COLORS.get(record.levelno, "")
        icon = self.ICONS.get(record.levelno, "   ")

        # Message
        message = record.getMessage()

        if not self._use_color:
            return f"{time_str}{ms_str} [{component:^8}] {icon} {message}"

        # Build colored output
        time_part = f"{self.DIM}{time_str}{self.RESET}{self.DIM}{ms_str}{self.RESET}"
        comp_part = f"{comp_color}{self.BOLD}[{component:^8}]{self.RESET}"
        icon_part = f"{level_color}{icon}{self.RESET}"
        msg_part = f"{level_color}{message}{self.RESET}"

        return f"{time_part} {comp_part} {icon_part} {msg_part}"


def setup_logging() -> None:
    """Configure logging with pretty formatting."""
    # Remove existing handlers
    root = logging.getLogger()
    for h in root.handlers[:]:
        root.removeHandler(h)

    # Create handler with pretty formatter
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(PrettyFormatter(use_color=LOG_COLOR))

    # Configure root logger
    logging.basicConfig(level=LOG_LEVEL, handlers=[handler], force=True)

    # Reduce noise from external libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


setup_logging()
logger = logging.getLogger(__name__)


def print_startup_banner() -> None:
    """Print a startup banner with configuration info."""
    banner = """
╔══════════════════════════════════════════════════════════════╗
║         Binance USD-M Market Data Server                     ║
╚══════════════════════════════════════════════════════════════╝"""
    print(banner)
    logger.info("Symbols: %s", ", ".join(SYMBOLS))
    logger.info("Backfill start: %s", BACKFILL_START_MS)
    logger.info("Log level: %s", LOG_LEVEL)


app = FastAPI(title="Binance USD-M Market Data Server")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
service = MarketDataService()
backfill_manager = BackfillManager()
_background_tasks: list[asyncio.Task[None]] = []


class BackfillRequest(BaseModel):
    symbol: str
    start_ms: int
    end_ms: int


@app.on_event("startup")
async def on_startup() -> None:
    print_startup_banner()
    logger.info("Initializing server...")

    # Ensure directories exist
    ensure_paths()

    # Initialize database
    logger.info("Initializing database...")
    init_db()
    logger.info("Database ready")

    # Set up pubsub event loop
    pubsub.set_event_loop(asyncio.get_running_loop())

    # Start live ingestion for each symbol
    for symbol in SYMBOLS:
        logger.info("Starting ingestion task for %s", symbol)
        _background_tasks.append(
            asyncio.create_task(run_live_ingest_with_backfill(symbol, BACKFILL_START_MS))
        )

    logger.info("Server startup complete - %d symbol(s) active", len(SYMBOLS))


@app.on_event("shutdown")
async def on_shutdown() -> None:
    logger.info("Shutting down server...")

    # Cancel all background tasks
    for task in _background_tasks:
        task.cancel()

    if _background_tasks:
        logger.info("Waiting for %d task(s) to complete...", len(_background_tasks))
        await asyncio.gather(*_background_tasks, return_exceptions=True)

    logger.info("Server shutdown complete")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/coverage", response_model=Coverage)
def get_coverage(symbol: str) -> Coverage:
    return service.get_coverage(symbol)


@app.post("/backfill/aggtrades")
def backfill_aggtrades(req: BackfillRequest) -> dict[str, str]:
    try:
        backfill_manager.ensure_aggtrades_range(req.symbol, req.start_ms, req.end_ms)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"status": "ok"}


@app.get("/candles", response_model=list[Candle])
def get_candles(
    symbol: str, start_ms: int, end_ms: int, interval: str = "1m"
) -> list[Candle]:
    try:
        interval_ms = parse_interval_to_ms(interval)
        aligned_start = start_ms - (start_ms % interval_ms)
        aligned_end = end_ms - (end_ms % interval_ms) + interval_ms - 1
        return service.get_candles(symbol, aligned_start, aligned_end, interval_ms)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/cvd", response_model=list[CvdCandle])
def get_cvd(
    symbol: str, start_ms: int, end_ms: int, interval: str = "1m"
) -> list[CvdCandle]:
    try:
        interval_ms = parse_interval_to_ms(interval)
        aligned_start = start_ms - (start_ms % interval_ms)
        aligned_end = end_ms - (end_ms % interval_ms) + interval_ms - 1
        return service.get_cvd(symbol, aligned_start, aligned_end, interval_ms)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/frvp", response_model=list[VolumeProfileBucket])
def get_frvp(
    symbol: str, start_ms: int, end_ms: int, bucket_size: float = 0.01
) -> list[VolumeProfileBucket]:
    try:
        return service.get_volume_profile(symbol, start_ms, end_ms, bucket_size)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _format_price_candle(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "t": row["minute_ts"],
        "o": row["open"],
        "h": row["high"],
        "l": row["low"],
        "c": row["close"],
        "v": row["volume"],
    }


def _format_cvd_candle(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "t": row["minute_ts"],
        "o": row["open"],
        "h": row["high"],
        "l": row["low"],
        "c": row["close"],
    }


@app.websocket("/ws/market")
async def ws_market(websocket: WebSocket) -> None:
    await websocket.accept()
    subscriptions: dict[tuple[str, str], dict[str, Any]] = {}

    async def consumer_task(
        symbol: str,
        timeframe: str,
        streams: list[str],
        queue: asyncio.Queue,
    ) -> None:
        while True:
            payload = await queue.get()
            price_candle = payload.get("price") if "price" in streams else None
            cvd_candle = payload.get("cvd") if "cvd" in streams else None
            if not price_candle and not cvd_candle:
                continue
            message: dict[str, Any] = {
                "type": "update",
                "symbol": symbol,
                "timeframe": timeframe,
            }
            if price_candle is not None:
                message["price"] = price_candle
            if cvd_candle is not None:
                message["cvd"] = cvd_candle
            await websocket.send_json(message)

    try:
        while True:
            message = await websocket.receive_json()
            msg_type = message.get("type")
            if msg_type == "ping":
                await websocket.send_json({"type": "pong", "id": message.get("id")})
                continue
            if msg_type == "subscribe":
                symbol = str(message.get("symbol", "")).upper()
                timeframe = str(message.get("timeframe", "")).strip().lower()
                streams = message.get("streams", ["price"])
                if not symbol or not timeframe:
                    await websocket.send_json(
                        {"type": "error", "message": "Missing symbol/timeframe", "code": "INVALID_REQUEST"}
                    )
                    continue
                if not isinstance(streams, list) or not streams:
                    await websocket.send_json(
                        {"type": "error", "message": "Missing streams", "code": "INVALID_REQUEST"}
                    )
                    continue
                try:
                    interval_ms = parse_interval_to_ms(timeframe)
                except ValueError:
                    await websocket.send_json(
                        {"type": "error", "message": "Invalid timeframe", "code": "INVALID_REQUEST"}
                    )
                    continue
                snapshot_limit = int(message.get("snapshot_limit", 200))
                snapshot_limit = max(1, min(snapshot_limit, 2000))
                key = (symbol, timeframe)
                if key in subscriptions:
                    await websocket.send_json(
                        {"type": "error", "message": "Already subscribed", "code": "INVALID_REQUEST"}
                    )
                    continue
                queue = pubsub.add_subscription(symbol, timeframe)
                task = asyncio.create_task(consumer_task(symbol, timeframe, streams, queue))
                subscriptions[key] = {
                    "queue": queue,
                    "task": task,
                    "streams": streams,
                }

                end_ms = int(time.time() * 1000)
                aligned_end = end_ms - (end_ms % interval_ms) + interval_ms - 1
                price_snapshot = await asyncio.to_thread(
                    service.get_recent_candles, symbol, aligned_end, interval_ms, snapshot_limit
                )
                cvd_snapshot = await asyncio.to_thread(
                    service.get_recent_cvd, symbol, aligned_end, interval_ms, snapshot_limit
                )
                snapshot_payload: dict[str, Any] = {
                    "type": "snapshot",
                    "symbol": symbol,
                    "timeframe": timeframe,
                }
                if "price" in streams:
                    snapshot_payload["price"] = [_format_price_candle(row) for row in price_snapshot]
                if "cvd" in streams:
                    snapshot_payload["cvd"] = [_format_cvd_candle(row) for row in cvd_snapshot]
                await websocket.send_json(snapshot_payload)
                await websocket.send_json(
                    {"type": "subscribed", "symbol": symbol, "timeframe": timeframe, "streams": streams}
                )
                continue
            if msg_type == "unsubscribe":
                symbol = str(message.get("symbol", "")).upper()
                timeframe = str(message.get("timeframe", "")).strip().lower()
                key = (symbol, timeframe)
                sub = subscriptions.pop(key, None)
                if sub is None:
                    await websocket.send_json(
                        {"type": "error", "message": "Not subscribed", "code": "INVALID_REQUEST"}
                    )
                    continue
                sub["task"].cancel()
                pubsub.remove_subscription(symbol, timeframe, sub["queue"])
                continue
            await websocket.send_json(
                {"type": "error", "message": "Unknown request type", "code": "INVALID_REQUEST"}
            )
    except WebSocketDisconnect:
        pass
    finally:
        for (symbol, timeframe), sub in list(subscriptions.items()):
            sub["task"].cancel()
            pubsub.remove_subscription(symbol, timeframe, sub["queue"])
