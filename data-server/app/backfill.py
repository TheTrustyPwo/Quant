from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import logging
from pathlib import Path

from app.binance_client import (
    download_vision_aggtrades_day,
    download_vision_aggtrades_month,
    fetch_recent_agg_trades,
    iter_vision_aggtrades_df,
    vision_day_available,
)
from app.config import VISION_DOWNLOAD_DIR
from app.db import insert_agg_trades
from app.ingest import rebuild_aggregates_from_range, store_trades_and_update_aggregates_sql

logger = logging.getLogger(__name__)

class BackfillManager:
    def __init__(self) -> None:
        self.vision_dir = Path(VISION_DOWNLOAD_DIR)

    def ensure_aggtrades_range(self, symbol: str, start_ms: int, end_ms: int) -> None:
        if end_ms < start_ms:
            raise ValueError("end_ms must be >= start_ms")
        symbol = symbol.upper()
        logger.info("Backfilling %s from %s to %s", symbol, start_ms, end_ms)
        start_date = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).date()
        end_date = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).date()
        today = datetime.now(tz=timezone.utc).date()
        current_month_start = date(today.year, today.month, 1)

        month_cursor = date(start_date.year, start_date.month, 1)
        while month_cursor <= end_date:
            next_month = (month_cursor.replace(day=28) + timedelta(days=4)).replace(day=1)
            month_end_date = next_month - timedelta(days=1)
            month_start_dt = datetime.combine(month_cursor, datetime.min.time(), tzinfo=timezone.utc)
            month_start_ms = int(month_start_dt.timestamp() * 1000)
            month_end_ms = month_start_ms + (month_end_date.day * 86_400_000) - 1
            range_start = max(start_ms, month_start_ms)
            range_end = min(end_ms, month_end_ms)

            if month_cursor < current_month_start:
                logger.info("Using Vision monthly backfill for %s %s-%02d", symbol, month_cursor.year, month_cursor.month)
                try:
                    processed = self.backfill_month_from_vision(
                        symbol, month_cursor.year, month_cursor.month, range_start, range_end
                    )
                    logger.info(
                        "Vision monthly backfill loaded %s trades for %s %s-%02d",
                        processed,
                        symbol,
                        month_cursor.year,
                        month_cursor.month,
                    )
                except Exception:
                    logger.warning(
                        "Monthly Vision backfill failed for %s %s-%02d, falling back to daily",
                        symbol,
                        month_cursor.year,
                        month_cursor.month,
                    )
                    self._backfill_days_in_month(symbol, range_start, range_end, month_cursor, month_end_date)
            else:
                self._backfill_days_in_month(symbol, range_start, range_end, month_cursor, month_end_date)

            month_cursor = next_month
        logger.info("Backfill complete for %s", symbol)

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
                logger.info(
                    "Vision backfill progress for %s %s: %s trades",
                    symbol,
                    label,
                    total_processed,
                )

        logger.info(
            "Loaded %s trades from Vision zip for %s %s (load %s)",
            total_processed,
            symbol,
            label,
            datetime.now(tz=timezone.utc) - load_start,
        )
        if total_processed:
            range_start = start_ms if start_ms is not None else min_ts
            range_end = end_ms if end_ms is not None else max_ts
            if range_start is None or range_end is None:
                return total_processed
            agg_start = datetime.now(tz=timezone.utc)
            logger.info(
                "Aggregating Vision range for %s %s (%s to %s)",
                symbol,
                label,
                range_start,
                range_end,
            )
            rebuild_aggregates_from_range(symbol, range_start, range_end)
            logger.info(
                "Aggregation done for %s %s (agg %s)",
                symbol,
                label,
                datetime.now(tz=timezone.utc) - agg_start,
            )
        return total_processed

    def _backfill_days_in_month(
        self,
        symbol: str,
        range_start: int,
        range_end: int,
        month_start: date,
        month_end: date,
    ) -> None:
        current = max(month_start, datetime.fromtimestamp(range_start / 1000, tz=timezone.utc).date())
        last = min(month_end, datetime.fromtimestamp(range_end / 1000, tz=timezone.utc).date())
        while current <= last:
            day_start = datetime.combine(current, datetime.min.time(), tzinfo=timezone.utc)
            day_start_ms = int(day_start.timestamp() * 1000)
            day_end_ms = day_start_ms + 86_400_000 - 1
            day_range_start = max(range_start, day_start_ms)
            day_range_end = min(range_end, day_end_ms)
            if vision_day_available(current):
                logger.info("Using Vision daily backfill for %s %s", symbol, current.isoformat())
                processed = self.backfill_day_from_vision(
                    symbol, current, day_range_start, day_range_end
                )
                logger.info(
                    "Vision daily backfill loaded %s trades for %s %s",
                    processed,
                    symbol,
                    current.isoformat(),
                )
            else:
                logger.info("Using REST backfill for %s %s", symbol, current.isoformat())
                rest_start = datetime.now(tz=timezone.utc)
                trades = fetch_recent_agg_trades(
                    symbol, day_range_start, day_range_end, show_progress=True
                )
                if trades:
                    logger.info(
                        "REST backfill fetched %s trades for %s %s (fetch %s)",
                        len(trades),
                        symbol,
                        current.isoformat(),
                        datetime.now(tz=timezone.utc) - rest_start,
                    )
                    filtered = [t for t in trades if day_range_start <= t.ts_ms <= day_range_end]
                    agg_start = datetime.now(tz=timezone.utc)
                    store_trades_and_update_aggregates_sql(
                        symbol, filtered, day_range_start, day_range_end
                    )
                    logger.info(
                        "REST backfill aggregated %s trades for %s %s (agg %s)",
                        len(filtered),
                        symbol,
                        current.isoformat(),
                        datetime.now(tz=timezone.utc) - agg_start,
                    )
                else:
                    logger.info(
                        "REST backfill returned no trades for %s %s",
                        symbol,
                        current.isoformat(),
                    )
            current += timedelta(days=1)
