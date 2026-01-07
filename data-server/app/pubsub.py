from __future__ import annotations

import asyncio
import threading
from typing import Iterable

SubscriptionKey = tuple[str, str]

_SUBSCRIPTIONS: dict[SubscriptionKey, list[asyncio.Queue]] = {}
_LOCK = threading.Lock()
_LOOP: asyncio.AbstractEventLoop | None = None


def set_event_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Store the main event loop for thread-safe publishing."""
    global _LOOP
    _LOOP = loop


def add_subscription(symbol: str, timeframe: str) -> asyncio.Queue:
    """Add a queue subscription for a symbol/timeframe."""
    key = (symbol, timeframe)
    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    with _LOCK:
        _SUBSCRIPTIONS.setdefault(key, []).append(queue)
    return queue


def remove_subscription(symbol: str, timeframe: str, queue: asyncio.Queue) -> None:
    """Remove a queue subscription for a symbol/timeframe."""
    key = (symbol, timeframe)
    with _LOCK:
        queues = _SUBSCRIPTIONS.get(key, [])
        if queue in queues:
            queues.remove(queue)
        if not queues and key in _SUBSCRIPTIONS:
            _SUBSCRIPTIONS.pop(key, None)


def get_active_timeframes(symbol: str) -> list[str]:
    """Return timeframes that currently have subscribers for the symbol."""
    with _LOCK:
        return [tf for (sym, tf) in _SUBSCRIPTIONS.keys() if sym == symbol]


async def publish_update(
    symbol: str,
    timeframe: str,
    price_candle: dict | None,
    cvd_candle: dict | None,
) -> None:
    """Publish a price/cvd update to subscribers for (symbol, timeframe)."""
    key = (symbol, timeframe)
    with _LOCK:
        queues = list(_SUBSCRIPTIONS.get(key, []))
    if not queues:
        return
    payload = {"price": price_candle, "cvd": cvd_candle}
    for queue in queues:
        try:
            queue.put_nowait(payload)
        except asyncio.QueueFull:
            continue


def publish_update_threadsafe(
    symbol: str,
    timeframe: str,
    price_candle: dict | None,
    cvd_candle: dict | None,
) -> None:
    """Publish from a non-async thread using the stored event loop."""
    if _LOOP is None:
        return
    _LOOP.call_soon_threadsafe(
        _LOOP.create_task, publish_update(symbol, timeframe, price_candle, cvd_candle)
    )
