from __future__ import annotations

import asyncio
import logging
import time

from app.db import (
    checkpoint_db,
    get_cvd_state,
    get_latest_cvd_close_before,
    get_last_agg_trade_ts,
    get_candles,
    get_cvd,
    insert_agg_trades,
    upsert_candles_from_agg_trades,
    upsert_cvd_from_agg_trades,
    upsert_candle_1m,
    upsert_cvd_1m,
    upsert_cvd_state,
)
from app.models import AggTrade
from app.utils import floor_minute, parse_interval_to_ms
from app import pubsub

logger = logging.getLogger(__name__)
_LIVE_STATS: dict[str, dict[str, object]] = {}


def _format_number(n: int | float) -> str:
    """Format a number with K/M suffixes for readability."""
    if abs(n) >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if abs(n) >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(int(n))


def _format_ts(ts_ms: int) -> str:
    """Format a timestamp for display."""
    from datetime import datetime, timezone
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime("%H:%M:%S")


def _record_live_batch(symbol: str, count: int, min_ts: int, max_ts: int) -> None:
    now = time.monotonic()
    stats = _LIVE_STATS.setdefault(
        symbol,
        {"count": 0, "min_ts": None, "max_ts": None, "last_log": now},
    )
    stats["count"] = int(stats["count"]) + count
    stats["min_ts"] = min_ts if stats["min_ts"] is None else min(stats["min_ts"], min_ts)
    stats["max_ts"] = max_ts if stats["max_ts"] is None else max(stats["max_ts"], max_ts)

    if now - float(stats["last_log"]) >= 60.0:
        trade_count = _format_number(stats["count"])
        time_range = f"{_format_ts(stats['min_ts'])} - {_format_ts(stats['max_ts'])}"
        logger.info(
            "%s | %s trades stored | Range: %s",
            symbol,
            trade_count,
            time_range,
        )
        try:
            checkpoint_db()
            logger.debug("%s | Database checkpoint completed", symbol)
        except Exception:
            logger.exception("%s | Checkpoint failed", symbol)
        stats["count"] = 0
        stats["min_ts"] = None
        stats["max_ts"] = None
        stats["last_log"] = now

def store_trades_and_update_aggregates(
    symbol: str,
    trades: list[AggTrade],
    assume_sorted: bool = False,
) -> None:
    """Store trades and update aggregates using Python-based calculation."""
    if not trades:
        return
    insert_agg_trades(trades)
    apply_trades_to_aggregates(symbol, trades, assume_sorted=assume_sorted)


def store_trades_and_update_aggregates_sql(
    symbol: str,
    trades: list[AggTrade],
    start_ms: int,
    end_ms: int,
) -> None:
    """Store trades and update aggregates using SQL-based calculation.

    This is more efficient for large batches as it leverages DuckDB's
    vectorized execution for aggregation.
    """
    if not trades:
        return

    # Align to minute boundaries
    aligned_start = start_ms - (start_ms % 60000)
    aligned_end = end_ms - (end_ms % 60000) + 59999

    # Get base CVD before inserting (to avoid reading uncommitted data)
    base_cvd = _starting_cvd(symbol, aligned_start)

    # Insert trades
    insert_agg_trades(trades)

    # Update aggregates in SQL (more efficient for larger batches)
    upsert_candles_from_agg_trades(symbol, aligned_start, aligned_end)
    last_cvd, last_ts = upsert_cvd_from_agg_trades(symbol, aligned_start, aligned_end, base_cvd)

    if last_cvd is not None and last_ts is not None:
        upsert_cvd_state(symbol, last_cvd, last_ts)

    # Publish updates to WebSocket subscribers
    _publish_updates(symbol, aligned_start, aligned_end)


def rebuild_aggregates_from_range(symbol: str, start_ms: int, end_ms: int) -> None:
    aligned_start = start_ms - (start_ms % 60000)
    aligned_end = end_ms - (end_ms % 60000) + 59999
    base_cvd = _starting_cvd(symbol, aligned_start)
    upsert_candles_from_agg_trades(symbol, aligned_start, aligned_end)
    last_cvd, last_ts = upsert_cvd_from_agg_trades(symbol, aligned_start, aligned_end, base_cvd)
    if last_cvd is not None and last_ts is not None:
        upsert_cvd_state(symbol, last_cvd, last_ts)
    _publish_updates(symbol, aligned_start, aligned_end)


def _publish_updates(symbol: str, start_ms: int, end_ms: int) -> None:
    timeframes = pubsub.get_active_timeframes(symbol)
    if not timeframes:
        return
    for timeframe in timeframes:
        try:
            interval_ms = parse_interval_to_ms(timeframe)
        except ValueError:
            continue
        aligned_start = start_ms - (start_ms % interval_ms)
        aligned_end = end_ms - (end_ms % interval_ms) + interval_ms - 1
        price_rows = get_candles(symbol, aligned_start, aligned_end, interval_ms)
        cvd_rows = get_cvd(symbol, aligned_start, aligned_end, interval_ms)
        price_map = {row["minute_ts"]: row for row in price_rows}
        cvd_map = {row["minute_ts"]: row for row in cvd_rows}
        for ts in sorted(set(price_map) | set(cvd_map)):
            price_row = price_map.get(ts)
            cvd_row = cvd_map.get(ts)
            price_candle = (
                {
                    "t": price_row["minute_ts"],
                    "o": price_row["open"],
                    "h": price_row["high"],
                    "l": price_row["low"],
                    "c": price_row["close"],
                    "v": price_row["volume"],
                }
                if price_row
                else None
            )
            cvd_candle = (
                {
                    "t": cvd_row["minute_ts"],
                    "o": cvd_row["open"],
                    "h": cvd_row["high"],
                    "l": cvd_row["low"],
                    "c": cvd_row["close"],
                }
                if cvd_row
                else None
            )
            pubsub.publish_update_threadsafe(symbol, timeframe, price_candle, cvd_candle)


def _starting_cvd(symbol: str, first_trade_ts: int) -> float:
    state = get_cvd_state(symbol)
    if state and state[1] <= first_trade_ts:
        return state[0]
    minute_ts = floor_minute(first_trade_ts)
    prior_close = get_latest_cvd_close_before(symbol, minute_ts)
    if prior_close is not None:
        return prior_close
    return 0.0


def apply_trades_to_aggregates(
    symbol: str,
    trades: list[AggTrade],
    assume_sorted: bool = False,
) -> None:
    """Apply trades to build OHLCV and CVD candles using Python.

    This is used for smaller batches or when SQL-based aggregation
    is not suitable. For large batches, use store_trades_and_update_aggregates_sql.
    """
    if not trades:
        return

    # Sort if needed
    ordered = trades if assume_sorted else sorted(trades, key=lambda t: (t.ts_ms, t.trade_id))

    # Initialize CVD from previous state
    running_cvd = _starting_cvd(symbol, ordered[0].ts_ms)

    # Use dicts for O(1) candle lookup
    candles: dict[int, dict[str, float]] = {}
    cvd_candles: dict[int, dict[str, float]] = {}
    last_ts = ordered[0].ts_ms

    # Process trades
    for trade in ordered:
        minute_ts = floor_minute(trade.ts_ms)

        # Update price candle
        candle = candles.get(minute_ts)
        if candle is None:
            candles[minute_ts] = {
                "open": trade.price,
                "high": trade.price,
                "low": trade.price,
                "close": trade.price,
                "volume": trade.qty,
            }
        else:
            if trade.price > candle["high"]:
                candle["high"] = trade.price
            if trade.price < candle["low"]:
                candle["low"] = trade.price
            candle["close"] = trade.price
            candle["volume"] += trade.qty

        # Update CVD
        delta = -trade.qty if trade.is_buyer_maker else trade.qty
        running_cvd += delta

        cvd_candle = cvd_candles.get(minute_ts)
        if cvd_candle is None:
            cvd_candles[minute_ts] = {
                "open": running_cvd,
                "high": running_cvd,
                "low": running_cvd,
                "close": running_cvd,
            }
        else:
            if running_cvd > cvd_candle["high"]:
                cvd_candle["high"] = running_cvd
            if running_cvd < cvd_candle["low"]:
                cvd_candle["low"] = running_cvd
            cvd_candle["close"] = running_cvd

        last_ts = trade.ts_ms

    # Batch upsert all candles at once
    candle_rows = [
        (symbol, minute_ts, v["open"], v["high"], v["low"], v["close"], v["volume"])
        for minute_ts, v in sorted(candles.items())
    ]
    cvd_rows = [
        (symbol, minute_ts, v["open"], v["high"], v["low"], v["close"])
        for minute_ts, v in sorted(cvd_candles.items())
    ]

    upsert_candle_1m(candle_rows)
    upsert_cvd_1m(cvd_rows)
    upsert_cvd_state(symbol, running_cvd, last_ts)


async def ingest_live_stream(symbol: str, batch_size: int = 500, flush_interval_ms: int = 1000) -> None:
    """Ingest live trades from WebSocket stream with batching.

    Args:
        symbol: Trading pair symbol
        batch_size: Maximum trades per batch
        flush_interval_ms: Force flush after this many milliseconds
    """
    from app.binance_client import stream_agg_trades

    buffer: list[AggTrade] = []
    last_flush_ts: int | None = None

    async for trade in stream_agg_trades(symbol):
        buffer.append(trade)

        # Check if we should flush
        should_flush = len(buffer) >= batch_size
        if not should_flush and last_flush_ts is not None:
            should_flush = (trade.ts_ms - last_flush_ts) >= flush_interval_ms

        if should_flush and buffer:
            min_ts = min(t.ts_ms for t in buffer)
            max_ts = max(t.ts_ms for t in buffer)
            store_trades_and_update_aggregates_sql(symbol, buffer, min_ts, max_ts)
            _record_live_batch(symbol, len(buffer), min_ts, max_ts)
            last_flush_ts = max_ts
            buffer = []

    # Final flush
    if buffer:
        min_ts = min(t.ts_ms for t in buffer)
        max_ts = max(t.ts_ms for t in buffer)
        store_trades_and_update_aggregates_sql(symbol, buffer, min_ts, max_ts)
        _record_live_batch(symbol, len(buffer), min_ts, max_ts)


async def _stream_to_queue(
    symbol: str,
    queue: asyncio.Queue[AggTrade],
    first_trade_future: asyncio.Future[AggTrade],
) -> None:
    from app.binance_client import stream_agg_trades

    reconnect_count = 0
    while True:
        try:
            if reconnect_count > 0:
                logger.info("%s | WebSocket reconnecting (attempt %d)", symbol, reconnect_count)
            async for trade in stream_agg_trades(symbol):
                if not first_trade_future.done():
                    first_trade_future.set_result(trade)
                    logger.debug("%s | First trade received at %s", symbol, _format_ts(trade.ts_ms))
                await queue.put(trade)
            reconnect_count = 0
        except asyncio.CancelledError:
            raise
        except Exception as e:
            reconnect_count += 1
            logger.warning("%s | WebSocket error: %s - reconnecting in 1s", symbol, type(e).__name__)
            await asyncio.sleep(1)


async def run_live_ingest_with_backfill(
    symbol: str,
    backfill_start_ms: int,
    batch_size: int = 1000,
) -> None:
    from datetime import datetime, timezone

    symbol = symbol.upper()
    queue: asyncio.Queue[AggTrade] = asyncio.Queue()
    loop = asyncio.get_running_loop()
    first_trade_future: asyncio.Future[AggTrade] = loop.create_future()
    stream_task = asyncio.create_task(_stream_to_queue(symbol, queue, first_trade_future))
    buffer: list[AggTrade] = []

    try:
        logger.info("%s | Connecting to WebSocket stream...", symbol)
        first_trade = await first_trade_future
        logger.info("%s | Connected - first trade at %s", symbol, _format_ts(first_trade.ts_ms))

        backfill_end_ms = first_trade.ts_ms - 1
        last_ts = get_last_agg_trade_ts(symbol)
        effective_start = backfill_start_ms

        if last_ts is not None and last_ts + 1 > effective_start:
            effective_start = last_ts + 1
            logger.info("%s | Resuming from existing data at %s", symbol, _format_ts(last_ts))

        if backfill_end_ms >= effective_start:
            start_dt = datetime.fromtimestamp(effective_start / 1000, tz=timezone.utc)
            end_dt = datetime.fromtimestamp(backfill_end_ms / 1000, tz=timezone.utc)
            logger.info(
                "%s | Starting backfill: %s -> %s",
                symbol,
                start_dt.strftime("%Y-%m-%d %H:%M"),
                end_dt.strftime("%Y-%m-%d %H:%M"),
            )
            from app.backfill import BackfillManager

            manager = BackfillManager()
            await asyncio.to_thread(
                manager.ensure_aggtrades_range, symbol, effective_start, backfill_end_ms
            )

        logger.info("%s | Live ingestion active", symbol)

        while True:
            trade = await queue.get()
            buffer.append(trade)
            if len(buffer) >= batch_size or queue.empty():
                batch = buffer
                buffer = []
                min_ts = min(t.ts_ms for t in batch)
                max_ts = max(t.ts_ms for t in batch)
                await asyncio.to_thread(
                    store_trades_and_update_aggregates_sql, symbol, batch, min_ts, max_ts
                )
                _record_live_batch(symbol, len(batch), min_ts, max_ts)
    except asyncio.CancelledError:
        logger.info("%s | Shutting down ingestion...", symbol)
        stream_task.cancel()
        await asyncio.gather(stream_task, return_exceptions=True)

        # Flush any remaining trades in buffer before shutdown
        if buffer:
            logger.info("%s | Flushing %d remaining trades...", symbol, len(buffer))
            try:
                min_ts = min(t.ts_ms for t in buffer)
                max_ts = max(t.ts_ms for t in buffer)
                await asyncio.to_thread(
                    store_trades_and_update_aggregates_sql, symbol, buffer, min_ts, max_ts
                )
                await asyncio.to_thread(checkpoint_db)
                logger.info("%s | Buffer flushed and checkpointed", symbol)
            except Exception:
                logger.exception("%s | Failed to flush buffer on shutdown", symbol)

        raise
