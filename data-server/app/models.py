from __future__ import annotations

from dataclasses import dataclass

from pydantic import BaseModel


@dataclass
class AggTrade:
    symbol: str
    trade_id: int
    price: float
    qty: float
    ts_ms: int
    is_buyer_maker: bool


class Candle(BaseModel):
    symbol: str
    minute_ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float


class CvdCandle(BaseModel):
    symbol: str
    minute_ts: int
    open: float
    high: float
    low: float
    close: float


class Coverage(BaseModel):
    symbol: str
    min_ts_ms: int | None
    max_ts_ms: int | None


class VolumeProfileBucket(BaseModel):
    price: float
    volume: float
