from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import logging
from pathlib import Path

from app.binance_client import (
    download_vision_aggtrades_day,
    download_vision_aggtrades_month,
    fetch_recent_agg_trades_df,
    iter_vision_aggtrades_df,
    vision_day_available,
)
from app.config import VISION_DOWNLOAD_DIR
from app.db import checkpoint_db, insert_agg_trades
from app.ingest import rebuild_aggregates_from_range, store_trades_and_update_aggregates_sql

logger = logging.getLogger(__name__)


def _format_number(n: int | float) -> str:
    """Format a number with K/M suffixes for readability."""
    if abs(n) >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if abs(n) >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(int(n))


def _format_duration(td: timedelta) -> str:
    """Format a timedelta for display."""
    total_secs = int(td.total_seconds())
    if total_secs < 60:
        return f"{total_secs}s"
    mins, secs = divmod(total_secs, 60)
    if mins < 60:
        return f"{mins}m {secs}s"
    hours, mins = divmod(mins, 60)
    return f"{hours}h {mins}m"

class BackfillManager:
    def __init__(self) -> None:
        self.vision_dir = Path(VISION_DOWNLOAD_DIR)

    def ensure_aggtrades_range(self, symbol: str, start_ms: int, end_ms: int) -> None:
        if end_ms < start_ms:
            raise ValueError("end_ms must be >= start_ms")
        symbol = symbol.upper()

        start_date = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).date()
        end_date = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).date()
        total_days = (end_date - start_date).days + 1

        logger.info(
            "%s | Backfill starting: %s to %s (%d days)",
            symbol,
            start_date.isoformat(),
            end_date.isoformat(),
            total_days,
        )

        today = datetime.now(tz=timezone.utc).date()
        current_month_start = date(today.year, today.month, 1)
        total_processed = 0
        backfill_start = datetime.now(tz=timezone.utc)

        month_cursor = date(start_date.year, start_date.month, 1)
        while month_cursor <= end_date:
            next_month = (month_cursor.replace(day=28) + timedelta(days=4)).replace(day=1)
            month_end_date = next_month - timedelta(days=1)
            month_start_dt = datetime.combine(month_cursor, datetime.min.time(), tzinfo=timezone.utc)
            month_start_ms = int(month_start_dt.timestamp() * 1000)
            month_end_ms = month_start_ms + (month_end_date.day * 86_400_000) - 1
            range_start = max(start_ms, month_start_ms)
            range_end = min(end_ms, month_end_ms)

            month_label = f"{month_cursor.year}-{month_cursor.month:02d}"

            if month_cursor < current_month_start:
                logger.info("%s | Processing month %s (Vision monthly)", symbol, month_label)
                try:
                    processed = self.backfill_month_from_vision(
                        symbol, month_cursor.year, month_cursor.month, range_start, range_end
                    )
                    total_processed += processed
                    logger.info(
                        "%s | Month %s complete: %s trades",
                        symbol,
                        month_label,
                        _format_number(processed),
                    )
                except Exception as e:
                    logger.warning(
                        "%s | Monthly Vision failed for %s (%s), using daily backfill",
                        symbol,
                        month_label,
                        type(e).__name__,
                    )
                    processed = self._backfill_days_in_month(symbol, range_start, range_end, month_cursor, month_end_date)
                    total_processed += processed
            else:
                processed = self._backfill_days_in_month(symbol, range_start, range_end, month_cursor, month_end_date)
                total_processed += processed

            month_cursor = next_month

        elapsed = datetime.now(tz=timezone.utc) - backfill_start
        logger.info(
            "%s | Backfill complete: %s trades in %s",
            symbol,
            _format_number(total_processed),
            _format_duration(elapsed),
        )

    def backfill_day_from_vision(
        self,
        symbol: str,
        date_value: date,
        start_ms: int | None = None,
        end_ms: int | None = None,
    ) -> int:
        symbol = symbol.upper()
        year = date_value.year
        month = date_value.month
        day = date_value.day
        dest_dir = self.vision_dir / symbol
        dest_dir.mkdir(parents=True, exist_ok=True)
        daily_name = f"{symbol}-aggTrades-{year}-{month:02d}-{day:02d}.zip"
        monthly_name = f"{symbol}-aggTrades-{year}-{month:02d}.zip"
        daily_path = dest_dir / daily_name
        monthly_path = dest_dir / monthly_name

        try:
            zip_path = download_vision_aggtrades_day(symbol, year, month, day, daily_path)
        except Exception:
            zip_path = download_vision_aggtrades_month(symbol, year, month, monthly_path)

        return self._load_vision_zip(symbol, zip_path, start_ms, end_ms, date_value.isoformat())

    def backfill_month_from_vision(
        self,
        symbol: str,
        year: int,
        month: int,
        start_ms: int | None = None,
        end_ms: int | None = None,
    ) -> int:
        symbol = symbol.upper()
        dest_dir = self.vision_dir / symbol
        dest_dir.mkdir(parents=True, exist_ok=True)
        monthly_name = f"{symbol}-aggTrades-{year}-{month:02d}.zip"
        monthly_path = dest_dir / monthly_name
        zip_path = download_vision_aggtrades_month(symbol, year, month, monthly_path)
        month_label = f"{year}-{month:02d}"
        return self._load_vision_zip(symbol, zip_path, start_ms, end_ms, month_label)

    def _load_vision_zip(
        self,
        symbol: str,
        zip_path: Path,
        start_ms: int | None,
        end_ms: int | None,
        label: str,
    ) -> int:
        load_start = datetime.now(tz=timezone.utc)
        total_processed = 0
        min_ts: int | None = None
        max_ts: int | None = None
        chunk_index = 0

        for chunk in iter_vision_aggtrades_df(
            zip_path, symbol, start_ms=start_ms, end_ms=end_ms
        ):
            if chunk.empty:
                continue
            chunk_index += 1
            total_processed += len(chunk)
            chunk_min = int(chunk["ts_ms"].min())
            chunk_max = int(chunk["ts_ms"].max())
            min_ts = chunk_min if min_ts is None else min(min_ts, chunk_min)
            max_ts = chunk_max if max_ts is None else max(max_ts, chunk_max)
            insert_agg_trades(chunk)
            if chunk_index % 5 == 0:
                logger.debug(
                    "%s | Vision %s: %s trades loaded...",
                    symbol,
                    label,
                    _format_number(total_processed),
                )
                checkpoint_db()

        load_elapsed = datetime.now(tz=timezone.utc) - load_start
        logger.info(
            "%s | Vision %s loaded: %s trades in %s",
            symbol,
            label,
            _format_number(total_processed),
            _format_duration(load_elapsed),
        )

        if total_processed:
            checkpoint_db()

        if total_processed:
            range_start = start_ms if start_ms is not None else min_ts
            range_end = end_ms if end_ms is not None else max_ts
            if range_start is None or range_end is None:
                return total_processed

            agg_start = datetime.now(tz=timezone.utc)
            logger.debug("%s | Aggregating candles for %s...", symbol, label)
            rebuild_aggregates_from_range(symbol, range_start, range_end)
            agg_elapsed = datetime.now(tz=timezone.utc) - agg_start
            logger.debug(
                "%s | Aggregation for %s complete in %s",
                symbol,
                label,
                _format_duration(agg_elapsed),
            )

        return total_processed

    def _backfill_days_in_month(
        self,
        symbol: str,
        range_start: int,
        range_end: int,
        month_start: date,
        month_end: date,
    ) -> int:
        current = max(month_start, datetime.fromtimestamp(range_start / 1000, tz=timezone.utc).date())
        last = min(month_end, datetime.fromtimestamp(range_end / 1000, tz=timezone.utc).date())
        total_days = (last - current).days + 1
        days_done = 0
        total_processed = 0

        while current <= last:
            day_start = datetime.combine(current, datetime.min.time(), tzinfo=timezone.utc)
            day_start_ms = int(day_start.timestamp() * 1000)
            day_end_ms = day_start_ms + 86_400_000 - 1
            day_range_start = max(range_start, day_start_ms)
            day_range_end = min(range_end, day_end_ms)
            days_done += 1
            day_label = current.isoformat()

            if vision_day_available(current):
                logger.debug(
                    "%s | Day %d/%d: %s (Vision daily)",
                    symbol,
                    days_done,
                    total_days,
                    day_label,
                )
                processed = self.backfill_day_from_vision(
                    symbol, current, day_range_start, day_range_end
                )
                total_processed += processed
            else:
                logger.info(
                    "%s | Day %d/%d: %s (REST API)",
                    symbol,
                    days_done,
                    total_days,
                    day_label,
                )
                rest_start = datetime.now(tz=timezone.utc)
                # Use DataFrame version for efficiency (avoids creating millions of Python objects)
                trades_df = fetch_recent_agg_trades_df(
                    symbol, day_range_start, day_range_end, show_progress=True
                )
                if not trades_df.empty:
                    fetch_elapsed = datetime.now(tz=timezone.utc) - rest_start
                    # Filter by timestamp range (vectorized, very fast)
                    mask = (trades_df["ts_ms"] >= day_range_start) & (trades_df["ts_ms"] <= day_range_end)
                    filtered_df = trades_df[mask]
                    num_trades = len(filtered_df)

                    logger.info(
                        "%s | REST %s: %s trades fetched in %s, inserting...",
                        symbol,
                        day_label,
                        _format_number(num_trades),
                        _format_duration(fetch_elapsed),
                    )

                    # Insert trades in chunks (like ZIP does) to avoid memory issues
                    # and provide progress feedback
                    CHUNK_SIZE = 500_000
                    insert_start = datetime.now(tz=timezone.utc)
                    for chunk_idx in range(0, num_trades, CHUNK_SIZE):
                        chunk_df = filtered_df.iloc[chunk_idx : chunk_idx + CHUNK_SIZE]
                        insert_agg_trades(chunk_df)
                        chunk_num = chunk_idx // CHUNK_SIZE + 1
                        total_chunks = (num_trades + CHUNK_SIZE - 1) // CHUNK_SIZE
                        if total_chunks > 1:
                            logger.info(
                                "%s | REST %s: inserted chunk %d/%d (%s trades)",
                                symbol,
                                day_label,
                                chunk_num,
                                total_chunks,
                                _format_number(len(chunk_df)),
                            )
                        # Checkpoint after each chunk (like ZIP does every 5 chunks)
                        if chunk_num % 5 == 0 or chunk_idx + CHUNK_SIZE >= num_trades:
                            checkpoint_db()

                    insert_elapsed = datetime.now(tz=timezone.utc) - insert_start
                    logger.info(
                        "%s | REST %s: %s trades inserted in %s, aggregating...",
                        symbol,
                        day_label,
                        _format_number(num_trades),
                        _format_duration(insert_elapsed),
                    )

                    # Now run aggregation (can be slow for large datasets)
                    agg_start = datetime.now(tz=timezone.utc)
                    rebuild_aggregates_from_range(symbol, day_range_start, day_range_end)
                    agg_elapsed = datetime.now(tz=timezone.utc) - agg_start
                    logger.info(
                        "%s | REST %s: aggregation complete in %s",
                        symbol,
                        day_label,
                        _format_duration(agg_elapsed),
                    )

                    total_processed += num_trades
                else:
                    logger.debug("%s | REST %s: no trades", symbol, day_label)

            checkpoint_db()
            current += timedelta(days=1)

        return total_processed
