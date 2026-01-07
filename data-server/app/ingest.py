from __future__ import annotations

import asyncio
import logging
import time

from app.db import (
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
        logger.info(
            "Live stored for %s: %s trades (%s to %s) in last minute",
            symbol,
            stats["count"],
            stats["min_ts"],
            stats["max_ts"],
        )
        stats["count"] = 0
        stats["min_ts"] = None
        stats["max_ts"] = None
        stats["last_log"] = now

def store_trades_and_update_aggregates(
    symbol: str,
    trades: list[AggTrade],
    assume_sorted: bool = False,
) -> None:
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
    if not trades:
        return
    aligned_start = start_ms - (start_ms % 60000)
    aligned_end = end_ms - (end_ms % 60000) + 59999
    insert_agg_trades(trades)
    base_cvd = _starting_cvd(symbol, aligned_start)
    upsert_candles_from_agg_trades(symbol, aligned_start, aligned_end)
    last_cvd, last_ts = upsert_cvd_from_agg_trades(symbol, aligned_start, aligned_end, base_cvd)
    if last_cvd is not None and last_ts is not None:
        upsert_cvd_state(symbol, last_cvd, last_ts)
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
    if not trades:
        return
    ordered = trades if assume_sorted else sorted(trades, key=lambda t: (t.ts_ms, t.trade_id))
    running_cvd = _starting_cvd(symbol, ordered[0].ts_ms)
    candles: dict[int, dict[str, float]] = {}
    cvd_candles: dict[int, dict[str, float]] = {}
    last_ts = ordered[0].ts_ms

    for trade in ordered:
        minute_ts = floor_minute(trade.ts_ms)
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
            candle["high"] = max(candle["high"], trade.price)
            candle["low"] = min(candle["low"], trade.price)
            candle["close"] = trade.price
            candle["volume"] += trade.qty

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
            cvd_candle["high"] = max(cvd_candle["high"], running_cvd)
            cvd_candle["low"] = min(cvd_candle["low"], running_cvd)
            cvd_candle["close"] = running_cvd
        last_ts = trade.ts_ms

    candle_rows = [
        (
            symbol,
            minute_ts,
            values["open"],
            values["high"],
            values["low"],
            values["close"],
            values["volume"],
        )
        for minute_ts, values in sorted(candles.items())
    ]
    cvd_rows = [
        (
            symbol,
            minute_ts,
            values["open"],
            values["high"],
            values["low"],
            values["close"],
        )
        for minute_ts, values in sorted(cvd_candles.items())
    ]

    upsert_candle_1m(candle_rows)
    upsert_cvd_1m(cvd_rows)
    upsert_cvd_state(symbol, running_cvd, last_ts)


async def ingest_live_stream(symbol: str, batch_size: int = 500) -> None:
    from app.binance_client import stream_agg_trades

    buffer: list[AggTrade] = []
    async for trade in stream_agg_trades(symbol):
        buffer.append(trade)
        if len(buffer) >= batch_size:
            min_ts = min(t.ts_ms for t in buffer)
            max_ts = max(t.ts_ms for t in buffer)
            store_trades_and_update_aggregates_sql(symbol, buffer, min_ts, max_ts)
            _record_live_batch(symbol, len(buffer), min_ts, max_ts)
            buffer = []
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

    while True:
        try:
            async for trade in stream_agg_trades(symbol):
                if not first_trade_future.done():
                    first_trade_future.set_result(trade)
                await queue.put(trade)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("WebSocket stream error for %s, retrying", symbol)
            await asyncio.sleep(1)


async def run_live_ingest_with_backfill(
    symbol: str,
    backfill_start_ms: int,
    batch_size: int = 1000,
) -> None:
    symbol = symbol.upper()
    queue: asyncio.Queue[AggTrade] = asyncio.Queue()
    loop = asyncio.get_running_loop()
    first_trade_future: asyncio.Future[AggTrade] = loop.create_future()
    stream_task = asyncio.create_task(_stream_to_queue(symbol, queue, first_trade_future))
    buffer: list[AggTrade] = []

    try:
        first_trade = await first_trade_future
        logger.info("First live trade for %s at %s", symbol, first_trade.ts_ms)
        backfill_end_ms = first_trade.ts_ms - 1
        last_ts = get_last_agg_trade_ts(symbol)
        effective_start = backfill_start_ms
        if last_ts is not None and last_ts + 1 > effective_start:
            effective_start = last_ts + 1
        if backfill_end_ms >= effective_start:
            logger.info(
                "Starting backfill for %s from %s to %s",
                symbol,
                effective_start,
                backfill_end_ms,
            )
            from app.backfill import BackfillManager

            manager = BackfillManager()
            await asyncio.to_thread(
                manager.ensure_aggtrades_range, symbol, effective_start, backfill_end_ms
            )
        logger.info("Live ingestion active for %s", symbol)

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
        stream_task.cancel()
        await asyncio.gather(stream_task, return_exceptions=True)
        raise
