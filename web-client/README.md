# Web Client

Simple TradingView Lightweight Charts UI for the market data server.

## Run locally

From `web-client/`:

```bash
python -m http.server 5173
```

Open `http://localhost:5173` and connect to `ws://localhost:8000/ws/market`.

## Notes

- Use the same `symbol` and `timeframe` values as the server.
- Scrolling left loads more history via the REST API.
