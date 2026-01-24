# Market Data Server + Web Client

This repo contains a local Binance USD-M market data server and a lightweight web chart client.

## Structure

- `data-server/` - FastAPI + DuckDB service that ingests aggTrades, builds candles/CVD, and exposes REST + WebSocket APIs.
- `web-client/` - TradingView Lightweight Charts UI that connects to the WebSocket and REST endpoints.

## Quick start (Docker)

From `data-server/`:

```bash
docker build -t binance-data-server .
docker run -it --rm \
  -e BINANCE_API_KEY=... \
  -e BINANCE_API_SECRET=... \
  -e SYMBOLS=BTCUSDT \
  -e BACKFILL_START_DATE=2024-01-01 \
  -v "./data:/app/data" \
  -p 8000:8000 \
  binance-data-server
```

## Web client

From the repo root:

```bash
cd web-client
python -m http.server 5173
```

Then open `http://localhost:5173` and connect to `ws://localhost:8000/ws/market`.

See `data-server/README.md` and `web-client/README.md` for details.
