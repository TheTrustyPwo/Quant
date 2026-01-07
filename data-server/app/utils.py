from __future__ import annotations

from datetime import datetime, timezone


def floor_minute(ts_ms: int) -> int:
    return (ts_ms // 60000) * 60000


def parse_ms(value: int | str) -> int:
    if isinstance(value, int):
        return value
    if not value.isdigit():
        raise ValueError("Only millisecond timestamps are supported")
    return int(value)


def utc_date_from_ms(ts_ms: int) -> datetime.date:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).date()


def parse_interval_to_ms(value: str) -> int:
    value = value.strip().lower()
    if value.isdigit():
        minutes = int(value)
        if minutes <= 0:
            raise ValueError("interval must be positive")
        return minutes * 60000
    if len(value) < 2:
        raise ValueError("invalid interval format")
    number_part = value[:-1]
    unit = value[-1]
    if not number_part.isdigit():
        raise ValueError("invalid interval format")
    amount = int(number_part)
    if amount <= 0:
        raise ValueError("interval must be positive")
    if unit == "m":
        return amount * 60000
    if unit == "h":
        return amount * 60 * 60000
    if unit == "d":
        return amount * 24 * 60 * 60000
    raise ValueError("invalid interval unit (use m, h, d)")
