# Binance USD-M Market Data Server

Local market data service for Binance USD-M futures. It stores aggTrades in DuckDB, pre-aggregates 1-minute OHLC and CVD candles, and exposes REST + WebSocket APIs for querying and streaming candles.

## Build and run with Docker

From `data-server/`:

```bash
docker build -t binance-data-server .
docker run -it --rm \
  -e BINANCE_API_KEY=... \
  -e BINANCE_API_SECRET=... \
  -e SYMBOLS=BTCUSDT \
  -e BACKFILL_START_DATE=2025-11-01 \
  -e LOG_LEVEL=INFO \
  -e LOG_COLOR=1 \
  -e BINANCE_WS_STREAM_BASE=wss://fstream.binance.com/ws \
  -v "./data:/app/data" \
  -p 8000:8000 \
  binance-data-server
```

Data is persisted in `data/` (DuckDB + Vision downloads) when the volume is mounted.

## REST API examples

```bash
# trigger a backfill
curl -X POST "http://localhost:8000/backfill/aggtrades" \
  -H "Content-Type: application/json" \
  -d '{"symbol": "BTCUSDT", "start_ms": 1704067200000, "end_ms": 1704153600000}'

# candles (interval can be 1m, 5m, 1h, 1d, or N minutes)
curl "http://localhost:8000/candles?symbol=BTCUSDT&start_ms=1704067200000&end_ms=1704153600000&interval=5m"

# CVD
curl "http://localhost:8000/cvd?symbol=BTCUSDT&start_ms=1704067200000&end_ms=1704153600000&interval=1h"

# fixed-range volume profile
curl "http://localhost:8000/frvp?symbol=BTCUSDT&start_ms=1704067200000&end_ms=1704153600000&bucket_size=0.01"
```

## WebSocket API

`ws://localhost:8000/ws/market`

```json
{ "type": "subscribe", "symbol": "BTCUSDT", "timeframe": "1m", "streams": ["price", "cvd"], "snapshot_limit": 200 }
```

## Web client

```bash
cd ../web-client
python -m http.server 5173
```

Open `http://localhost:5173` and connect to `ws://localhost:8000/ws/market`.

## Plot test script

```bash
# Requires mplfinance in your local environment
python scripts/plot_5m.py --base-url http://localhost:8000 --symbol BTCUSDT --interval 5m --hours 6
```

## Configuration

- `BACKFILL_START_DATE` (UTC, `YYYY-MM-DD`)
- `SYMBOLS` (comma-separated list, defaults to `BTCUSDT`)
- `DUCKDB_PATH` and `VISION_DOWNLOAD_DIR` for storage locations
- `BINANCE_WS_STREAM_BASE` if you need a custom stream base URL
- `REST_SLEEP_SEC`, `REST_MAX_RETRIES`, `REST_BACKOFF_SEC` for REST backfill throttling

## Notes

- All timestamps are milliseconds since epoch (UTC). Query params accept ms only.
- Vision backfills use daily or monthly aggTrades zips. Yesterday's data is only used after 08:00 UTC.
- On startup the server opens a WebSocket per configured symbol, captures the first aggTrade timestamp, backfills up to it, then continuously streams into DuckDB with incremental aggregates.
