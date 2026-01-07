from __future__ import annotations

from app.db import (
    get_agg_trades_coverage,
    get_candles,
    get_cvd,
    get_recent_candles,
    get_recent_cvd,
    get_volume_profile,
)
from app.models import Coverage


class MarketDataService:
    def get_coverage(self, symbol: str) -> Coverage:
        symbol = symbol.upper()
        min_ts, max_ts = get_agg_trades_coverage(symbol)
        return Coverage(symbol=symbol, min_ts_ms=min_ts, max_ts_ms=max_ts)

    def get_candles(
        self, symbol: str, start_ms: int, end_ms: int, interval_ms: int
    ) -> list[dict[str, object]]:
        if end_ms < start_ms:
            raise ValueError("end_ms must be >= start_ms")
        return get_candles(symbol.upper(), start_ms, end_ms, interval_ms)

    def get_cvd(
        self, symbol: str, start_ms: int, end_ms: int, interval_ms: int
    ) -> list[dict[str, object]]:
        if end_ms < start_ms:
            raise ValueError("end_ms must be >= start_ms")
        return get_cvd(symbol.upper(), start_ms, end_ms, interval_ms)

    def get_volume_profile(
        self, symbol: str, start_ms: int, end_ms: int, bucket_size: float
    ) -> list[dict[str, object]]:
        if end_ms < start_ms:
            raise ValueError("end_ms must be >= start_ms")
        return get_volume_profile(symbol.upper(), start_ms, end_ms, bucket_size)

    def get_recent_candles(
        self, symbol: str, end_ms: int, interval_ms: int, limit: int
    ) -> list[dict[str, object]]:
        return get_recent_candles(symbol.upper(), end_ms, interval_ms, limit)

    def get_recent_cvd(
        self, symbol: str, end_ms: int, interval_ms: int, limit: int
    ) -> list[dict[str, object]]:
        return get_recent_cvd(symbol.upper(), end_ms, interval_ms, limit)
