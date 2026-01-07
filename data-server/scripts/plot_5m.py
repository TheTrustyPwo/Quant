import argparse
from datetime import datetime, timedelta, timezone

import httpx
import mplfinance as mpf
import pandas as pd


def build_mpf_df(df):
    if df.empty:
        return df
    df = df.copy()
    df["dt"] = pd.to_datetime(df["minute_ts"], unit="ms", utc=True)
    df = df.set_index("dt")
    df = df.rename(
        columns={
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
        }
    )
    return df[["Open", "High", "Low", "Close"] + (["Volume"] if "Volume" in df.columns else [])]



def main():
    parser = argparse.ArgumentParser(description="Plot 5m candles and CVD from the API")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--symbol", default="SOLUSDT")
    parser.add_argument("--hours", type=int, default=6)
    parser.add_argument("--interval", default="5m")
    parser.add_argument("--bucket-size", type=float, default=0.01)
    args = parser.parse_args()

    now = datetime.now(tz=timezone.utc)
    start = now - timedelta(hours=args.hours)
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(now.timestamp() * 1000)

    params = {
        "symbol": args.symbol,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "interval": args.interval,
    }

    candles_resp = httpx.get(f"{args.base_url}/candles", params=params, timeout=30)
    candles_resp.raise_for_status()
    candles = candles_resp.json()

    cvd_resp = httpx.get(f"{args.base_url}/cvd", params=params, timeout=30)
    cvd_resp.raise_for_status()
    cvd = cvd_resp.json()

    frvp_params = {
        "symbol": args.symbol,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "bucket_size": args.bucket_size,
    }
    frvp_resp = httpx.get(f"{args.base_url}/frvp", params=frvp_params, timeout=30)
    frvp_resp.raise_for_status()
    frvp = frvp_resp.json()

    candle_df = pd.DataFrame(candles)
    if candle_df.empty:
        print("No candle data returned")
        return
    candle_mpf = build_mpf_df(candle_df)

    cvd_df = pd.DataFrame(cvd)
    cvd_mpf = build_mpf_df(cvd_df)
    frvp_df = pd.DataFrame(frvp)

    fig = mpf.figure(style="yahoo", figsize=(12, 8))
    ax1 = fig.add_subplot(2, 1, 1)
    ax2 = fig.add_subplot(2, 1, 2, sharex=ax1)

    mpf.plot(
        candle_mpf,
        type="candle",
        ax=ax1,
        volume=False,
        show_nontrading=True,
    )
    ax1.set_title(f"{args.symbol} {args.interval} Candles")

    if not frvp_df.empty:
        ax_profile = ax1.twiny()
        ax_profile.barh(
            frvp_df["price"],
            frvp_df["volume"],
            color="gray",
            alpha=0.3,
            height=args.bucket_size * 0.9,
        )
        ax_profile.set_xlim(0, frvp_df["volume"].max() * 1.05)
        ax_profile.invert_xaxis()
        ax_profile.set_xticks([])
        ax_profile.set_xlabel("FRVP", labelpad=6)

    if not cvd_mpf.empty:
        mpf.plot(
            cvd_mpf,
            type="candle",
            ax=ax2,
            volume=False,
            show_nontrading=True,
        )
    ax2.set_title(f"{args.symbol} {args.interval} CVD")

    mpf.show()


if __name__ == "__main__":
    main()
