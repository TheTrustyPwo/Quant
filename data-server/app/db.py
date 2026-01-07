from __future__ import annotations

import threading
from typing import Iterable

import duckdb
import pandas as pd

from app.config import DUCKDB_PATH
from app.models import AggTrade

_THREAD_LOCAL = threading.local()
_LOCK = threading.Lock()


def get_connection() -> duckdb.DuckDBPyConnection:
    conn = getattr(_THREAD_LOCAL, "conn", None)
    if conn is None:
        conn = duckdb.connect(DUCKDB_PATH)
        try:
            conn.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        except Exception:
            pass
        _THREAD_LOCAL.conn = conn
        _init_schema(conn)
    return conn


def _init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    with _LOCK:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agg_trades (
                symbol TEXT NOT NULL,
                trade_id BIGINT NOT NULL,
                price DOUBLE NOT NULL,
                qty DOUBLE NOT NULL,
                ts_ms BIGINT NOT NULL,
                is_buyer_maker BOOLEAN NOT NULL,
                PRIMARY KEY (symbol, trade_id)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS candles_1m (
                symbol TEXT NOT NULL,
                minute_ts BIGINT NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                volume DOUBLE NOT NULL,
                PRIMARY KEY (symbol, minute_ts)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cvd_1m (
                symbol TEXT NOT NULL,
                minute_ts BIGINT NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                PRIMARY KEY (symbol, minute_ts)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cvd_state (
                symbol TEXT PRIMARY KEY,
                last_cvd DOUBLE NOT NULL,
                last_ts_ms BIGINT NOT NULL
            );
            """
        )


def init_db(conn: duckdb.DuckDBPyConnection | None = None) -> None:
    if conn is None:
        conn = get_connection()
    _init_schema(conn)


def insert_agg_trades(trades: list[AggTrade] | pd.DataFrame) -> int:
    if trades is None:
        return 0
    if isinstance(trades, pd.DataFrame):
        df = trades
    else:
        if not trades:
            return 0
        df = pd.DataFrame(
            {
                "symbol": [t.symbol for t in trades],
                "trade_id": [t.trade_id for t in trades],
                "price": [t.price for t in trades],
                "qty": [t.qty for t in trades],
                "ts_ms": [t.ts_ms for t in trades],
                "is_buyer_maker": [t.is_buyer_maker for t in trades],
            }
        )
    if df.empty:
        return 0
    conn = get_connection()
    with _LOCK:
        conn.register("df_trades", df)
        conn.execute(
            """
            INSERT OR REPLACE INTO agg_trades
            SELECT symbol, trade_id, price, qty, ts_ms, is_buyer_maker
            FROM df_trades
            """
        )
        conn.unregister("df_trades")
    return len(df)


def upsert_candle_1m(rows: Iterable[tuple[str, int, float, float, float, float, float]]) -> None:
    rows_list = list(rows)
    if not rows_list:
        return
    df = pd.DataFrame(
        rows_list,
        columns=["symbol", "minute_ts", "open", "high", "low", "close", "volume"],
    )
    conn = get_connection()
    with _LOCK:
        conn.register("df_candles", df)
        conn.execute(
            """
            INSERT OR REPLACE INTO candles_1m
            SELECT symbol, minute_ts, open, high, low, close, volume
            FROM df_candles
            """
        )
        conn.unregister("df_candles")


def upsert_cvd_1m(rows: Iterable[tuple[str, int, float, float, float, float]]) -> None:
    rows_list = list(rows)
    if not rows_list:
        return
    df = pd.DataFrame(
        rows_list, columns=["symbol", "minute_ts", "open", "high", "low", "close"]
    )
    conn = get_connection()
    with _LOCK:
        conn.register("df_cvd", df)
        conn.execute(
            """
            INSERT OR REPLACE INTO cvd_1m
            SELECT symbol, minute_ts, open, high, low, close
            FROM df_cvd
            """
        )
        conn.unregister("df_cvd")


def upsert_candles_from_agg_trades(symbol: str, start_ms: int, end_ms: int) -> None:
    conn = get_connection()
    with _LOCK:
        conn.execute(
            """
            INSERT OR REPLACE INTO candles_1m
            SELECT symbol, minute_ts, open, high, low, close, volume
            FROM (
                SELECT
                    symbol,
                    ts_ms - (ts_ms % 60000) AS minute_ts,
                    arg_min(price, struct_pack(ts_ms := ts_ms, trade_id := trade_id)) AS open,
                    max(price) AS high,
                    min(price) AS low,
                    arg_max(price, struct_pack(ts_ms := ts_ms, trade_id := trade_id)) AS close,
                    sum(qty) AS volume
                FROM (
                    SELECT
                        symbol,
                        price,
                        qty,
                        ts_ms,
                        trade_id
                    FROM agg_trades
                    WHERE symbol = ? AND ts_ms BETWEEN ? AND ?
                ) t
                GROUP BY symbol, minute_ts
            ) agg;
            """,
            [symbol, start_ms, end_ms],
        )


def upsert_cvd_from_agg_trades(
    symbol: str, start_ms: int, end_ms: int, base_cvd: float
) -> tuple[float | None, int | None]:
    conn = get_connection()
    with _LOCK:
        row = conn.execute(
            """
            SELECT
                SUM(CASE WHEN is_buyer_maker THEN -qty ELSE qty END) AS delta_sum,
                MAX(ts_ms) AS max_ts
            FROM agg_trades
            WHERE symbol = ? AND ts_ms BETWEEN ? AND ?
            """,
            [symbol, start_ms, end_ms],
        ).fetchone()
        if not row or row[0] is None or row[1] is None:
            return None, None
        delta_sum = float(row[0])
        last_ts = int(row[1])

        conn.execute(
            """
            INSERT OR REPLACE INTO cvd_1m
            SELECT symbol, minute_ts,
                   arg_min(cvd, order_key) AS open,
                   max(cvd) AS high,
                   min(cvd) AS low,
                   arg_max(cvd, order_key) AS close
            FROM (
                SELECT
                    symbol,
                    minute_ts,
                    order_key,
                    SUM(delta) OVER (
                        ORDER BY ts_ms, trade_id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) + ? AS cvd
                FROM (
                    SELECT
                        symbol,
                        ts_ms,
                        trade_id,
                        ts_ms - (ts_ms % 60000) AS minute_ts,
                        struct_pack(ts_ms := ts_ms, trade_id := trade_id) AS order_key,
                        CASE WHEN is_buyer_maker THEN -qty ELSE qty END AS delta
                    FROM agg_trades
                    WHERE symbol = ? AND ts_ms BETWEEN ? AND ?
                ) base
            ) cvd_series
            GROUP BY symbol, minute_ts;
            """,
            [base_cvd, symbol, start_ms, end_ms],
        )
    last_cvd = base_cvd + delta_sum
    return last_cvd, last_ts


def get_agg_trades(symbol: str, start_ms: int, end_ms: int) -> list[AggTrade]:
    conn = get_connection()
    with _LOCK:
        rows = conn.execute(
            """
            SELECT symbol, trade_id, price, qty, ts_ms, is_buyer_maker
            FROM agg_trades
            WHERE symbol = ? AND ts_ms BETWEEN ? AND ?
            ORDER BY ts_ms ASC, trade_id ASC
            """,
            [symbol, start_ms, end_ms],
        ).fetchall()
    return [
        AggTrade(
            symbol=row[0],
            trade_id=int(row[1]),
            price=float(row[2]),
            qty=float(row[3]),
            ts_ms=int(row[4]),
            is_buyer_maker=bool(row[5]),
        )
        for row in rows
    ]


def get_candles_1m(symbol: str, start_ms: int, end_ms: int) -> list[dict[str, object]]:
    conn = get_connection()
    with _LOCK:
        rows = conn.execute(
            """
            SELECT symbol, minute_ts, open, high, low, close, volume
            FROM candles_1m
            WHERE symbol = ? AND minute_ts BETWEEN ? AND ?
            ORDER BY minute_ts ASC
            """,
            [symbol, start_ms, end_ms],
        ).fetchall()
    return [
        {
            "symbol": row[0],
            "minute_ts": int(row[1]),
            "open": float(row[2]),
            "high": float(row[3]),
            "low": float(row[4]),
            "close": float(row[5]),
            "volume": float(row[6]),
        }
        for row in rows
    ]


def get_candles(
    symbol: str, start_ms: int, end_ms: int, interval_ms: int
) -> list[dict[str, object]]:
    if interval_ms <= 60000:
        return get_candles_1m(symbol, start_ms, end_ms)
    conn = get_connection()
    with _LOCK:
        rows = conn.execute(
            """
            SELECT symbol, bucket_ts AS minute_ts,
                   arg_min(open, minute_ts) AS open,
                   max(high) AS high,
                   min(low) AS low,
                   arg_max(close, minute_ts) AS close,
                   sum(volume) AS volume
            FROM (
                SELECT
                    symbol,
                    minute_ts,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    minute_ts - (minute_ts % ?) AS bucket_ts
                FROM candles_1m
                WHERE symbol = ? AND minute_ts BETWEEN ? AND ?
            ) t
            GROUP BY symbol, bucket_ts
            ORDER BY bucket_ts ASC
            """,
            [interval_ms, symbol, start_ms, end_ms],
        ).fetchall()
    return [
        {
            "symbol": row[0],
            "minute_ts": int(row[1]),
            "open": float(row[2]),
            "high": float(row[3]),
            "low": float(row[4]),
            "close": float(row[5]),
            "volume": float(row[6]),
        }
        for row in rows
    ]


def get_cvd_1m(symbol: str, start_ms: int, end_ms: int) -> list[dict[str, object]]:
    conn = get_connection()
    with _LOCK:
        rows = conn.execute(
            """
            SELECT symbol, minute_ts, open, high, low, close
            FROM cvd_1m
            WHERE symbol = ? AND minute_ts BETWEEN ? AND ?
            ORDER BY minute_ts ASC
            """,
            [symbol, start_ms, end_ms],
        ).fetchall()
    return [
        {
            "symbol": row[0],
            "minute_ts": int(row[1]),
            "open": float(row[2]),
            "high": float(row[3]),
            "low": float(row[4]),
            "close": float(row[5]),
        }
        for row in rows
    ]


def get_cvd(symbol: str, start_ms: int, end_ms: int, interval_ms: int) -> list[dict[str, object]]:
    if interval_ms <= 60000:
        return get_cvd_1m(symbol, start_ms, end_ms)
    conn = get_connection()
    with _LOCK:
        rows = conn.execute(
            """
            SELECT symbol, bucket_ts AS minute_ts,
                   arg_min(open, minute_ts) AS open,
                   max(high) AS high,
                   min(low) AS low,
                   arg_max(close, minute_ts) AS close
            FROM (
                SELECT
                    symbol,
                    minute_ts,
                    open,
                    high,
                    low,
                    close,
                    minute_ts - (minute_ts % ?) AS bucket_ts
                FROM cvd_1m
                WHERE symbol = ? AND minute_ts BETWEEN ? AND ?
            ) t
            GROUP BY symbol, bucket_ts
            ORDER BY bucket_ts ASC
            """,
            [interval_ms, symbol, start_ms, end_ms],
        ).fetchall()
    return [
        {
            "symbol": row[0],
            "minute_ts": int(row[1]),
            "open": float(row[2]),
            "high": float(row[3]),
            "low": float(row[4]),
            "close": float(row[5]),
        }
        for row in rows
    ]


def get_recent_candles(
    symbol: str, end_ms: int, interval_ms: int, limit: int
) -> list[dict[str, object]]:
    if limit <= 0:
        return []
    if interval_ms <= 60000:
        conn = get_connection()
        with _LOCK:
            rows = conn.execute(
                """
                SELECT symbol, minute_ts, open, high, low, close, volume
                FROM candles_1m
                WHERE symbol = ? AND minute_ts <= ?
                ORDER BY minute_ts DESC
                LIMIT ?
                """,
                [symbol, end_ms, limit],
            ).fetchall()
        rows = list(reversed(rows))
        return [
            {
                "symbol": row[0],
                "minute_ts": int(row[1]),
                "open": float(row[2]),
                "high": float(row[3]),
                "low": float(row[4]),
                "close": float(row[5]),
                "volume": float(row[6]),
            }
            for row in rows
        ]

    conn = get_connection()
    with _LOCK:
        rows = conn.execute(
            """
            SELECT symbol, bucket_ts AS minute_ts,
                   arg_min(open, minute_ts) AS open,
                   max(high) AS high,
                   min(low) AS low,
                   arg_max(close, minute_ts) AS close,
                   sum(volume) AS volume
            FROM (
                SELECT
                    symbol,
                    minute_ts,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    minute_ts - (minute_ts % ?) AS bucket_ts
                FROM candles_1m
                WHERE symbol = ? AND minute_ts <= ?
            ) t
            GROUP BY symbol, bucket_ts
            ORDER BY bucket_ts DESC
            LIMIT ?
            """,
            [interval_ms, symbol, end_ms, limit],
        ).fetchall()
    rows = list(reversed(rows))
    return [
        {
            "symbol": row[0],
            "minute_ts": int(row[1]),
            "open": float(row[2]),
            "high": float(row[3]),
            "low": float(row[4]),
            "close": float(row[5]),
            "volume": float(row[6]),
        }
        for row in rows
    ]


def get_recent_cvd(
    symbol: str, end_ms: int, interval_ms: int, limit: int
) -> list[dict[str, object]]:
    if limit <= 0:
        return []
    if interval_ms <= 60000:
        conn = get_connection()
        with _LOCK:
            rows = conn.execute(
                """
                SELECT symbol, minute_ts, open, high, low, close
                FROM cvd_1m
                WHERE symbol = ? AND minute_ts <= ?
                ORDER BY minute_ts DESC
                LIMIT ?
                """,
                [symbol, end_ms, limit],
            ).fetchall()
        rows = list(reversed(rows))
        return [
            {
                "symbol": row[0],
                "minute_ts": int(row[1]),
                "open": float(row[2]),
                "high": float(row[3]),
                "low": float(row[4]),
                "close": float(row[5]),
            }
            for row in rows
        ]

    conn = get_connection()
    with _LOCK:
        rows = conn.execute(
            """
            SELECT symbol, bucket_ts AS minute_ts,
                   arg_min(open, minute_ts) AS open,
                   max(high) AS high,
                   min(low) AS low,
                   arg_max(close, minute_ts) AS close
            FROM (
                SELECT
                    symbol,
                    minute_ts,
                    open,
                    high,
                    low,
                    close,
                    minute_ts - (minute_ts % ?) AS bucket_ts
                FROM cvd_1m
                WHERE symbol = ? AND minute_ts <= ?
            ) t
            GROUP BY symbol, bucket_ts
            ORDER BY bucket_ts DESC
            LIMIT ?
            """,
            [interval_ms, symbol, end_ms, limit],
        ).fetchall()
    rows = list(reversed(rows))
    return [
        {
            "symbol": row[0],
            "minute_ts": int(row[1]),
            "open": float(row[2]),
            "high": float(row[3]),
            "low": float(row[4]),
            "close": float(row[5]),
        }
        for row in rows
    ]


def get_volume_profile(
    symbol: str, start_ms: int, end_ms: int, bucket_size: float
) -> list[dict[str, object]]:
    if bucket_size <= 0:
        raise ValueError("bucket_size must be positive")
    conn = get_connection()
    with _LOCK:
        rows = conn.execute(
            """
            SELECT price_bucket, sum(qty) AS volume
            FROM (
                SELECT
                    floor(price / ?) * ? AS price_bucket,
                    qty
                FROM agg_trades
                WHERE symbol = ? AND ts_ms BETWEEN ? AND ?
            ) t
            GROUP BY price_bucket
            ORDER BY price_bucket ASC
            """,
            [bucket_size, bucket_size, symbol, start_ms, end_ms],
        ).fetchall()
    return [{"price": float(row[0]), "volume": float(row[1])} for row in rows]


def get_last_agg_trade_ts(symbol: str) -> int | None:
    conn = get_connection()
    with _LOCK:
        row = conn.execute(
            "SELECT MAX(ts_ms) FROM agg_trades WHERE symbol = ?",
            [symbol],
        ).fetchone()
    if not row:
        return None
    return int(row[0]) if row[0] is not None else None


def get_agg_trades_coverage(symbol: str) -> tuple[int | None, int | None]:
    conn = get_connection()
    with _LOCK:
        row = conn.execute(
            "SELECT MIN(ts_ms), MAX(ts_ms) FROM agg_trades WHERE symbol = ?",
            [symbol],
        ).fetchone()
    if not row:
        return None, None
    min_ts = int(row[0]) if row[0] is not None else None
    max_ts = int(row[1]) if row[1] is not None else None
    return min_ts, max_ts


def get_cvd_state(symbol: str) -> tuple[float, int] | None:
    conn = get_connection()
    with _LOCK:
        row = conn.execute(
            "SELECT last_cvd, last_ts_ms FROM cvd_state WHERE symbol = ?",
            [symbol],
        ).fetchone()
    if not row:
        return None
    return float(row[0]), int(row[1])


def upsert_cvd_state(symbol: str, last_cvd: float, last_ts_ms: int) -> None:
    conn = get_connection()
    with _LOCK:
        conn.execute(
            """
            INSERT OR REPLACE INTO cvd_state (symbol, last_cvd, last_ts_ms)
            VALUES (?, ?, ?)
            """,
            [symbol, last_cvd, last_ts_ms],
        )


def get_latest_cvd_close_before(symbol: str, minute_ts: int) -> float | None:
    conn = get_connection()
    with _LOCK:
        row = conn.execute(
            """
            SELECT close FROM cvd_1m
            WHERE symbol = ? AND minute_ts < ?
            ORDER BY minute_ts DESC
            LIMIT 1
            """,
            [symbol, minute_ts],
        ).fetchone()
    if not row:
        return None
    return float(row[0])
