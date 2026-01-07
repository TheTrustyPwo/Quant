const priceContainer = document.getElementById("price-chart");
const cvdContainer = document.getElementById("cvd-chart");
const statusEl = document.getElementById("status");
const connectBtn = document.getElementById("connect");
const disconnectBtn = document.getElementById("disconnect");
const symbolInput = document.getElementById("symbol");
const timeframeInput = document.getElementById("timeframe");
const snapshotInput = document.getElementById("snapshot");
const wsUrlInput = document.getElementById("ws-url");

const defaultHost = window.location.hostname || "localhost";
const defaultScheme = window.location.protocol === "https:" ? "wss" : "ws";
const defaultWsUrl = `${defaultScheme}://${defaultHost}:8000/ws/market`;
wsUrlInput.value = defaultWsUrl;

const chartOptions = {
  layout: {
    background: { color: "#101218" },
    textColor: "#d7dce5",
  },
  watermark: { visible: false },
  grid: {
    vertLines: { color: "#1f222b" },
    horzLines: { color: "#1f222b" },
  },
  timeScale: { rightOffset: 5, timeVisible: true, secondsVisible: false },
  crosshair: { mode: 1 },
};

const priceChart = LightweightCharts.createChart(priceContainer, chartOptions);
priceChart.priceScale("right").applyOptions({
  scaleMargins: { top: 0.1, bottom: 0.25 },
});
const priceSeries = typeof priceChart.addCandlestickSeries === "function"
  ? priceChart.addCandlestickSeries()
  : priceChart.addSeries(LightweightCharts.CandlestickSeries, {});
const volumeSeries = typeof priceChart.addHistogramSeries === "function"
  ? priceChart.addHistogramSeries({
      color: "#4b5563",
      priceFormat: { type: "volume" },
      priceScaleId: "",
    })
  : priceChart.addSeries(LightweightCharts.HistogramSeries, {
      color: "#4b5563",
      priceFormat: { type: "volume" },
      priceScaleId: "",
    });
volumeSeries.priceScale().applyOptions({
  scaleMargins: { top: 0.75, bottom: 0 },
});

const cvdChart = LightweightCharts.createChart(cvdContainer, chartOptions);
const cvdSeries = typeof cvdChart.addCandlestickSeries === "function"
  ? cvdChart.addCandlestickSeries({ upColor: "#26a69a", downColor: "#ef5350" })
  : cvdChart.addSeries(LightweightCharts.CandlestickSeries, { upColor: "#26a69a", downColor: "#ef5350" });
cvdSeries.applyOptions({
  priceFormat: {
    type: "custom",
    formatter: (value) => formatCompact(value),
  },
});

function resizeCharts() {
  const width = priceContainer.clientWidth;
  priceChart.applyOptions({ width });
  cvdChart.applyOptions({ width });
}

window.addEventListener("resize", resizeCharts);
resizeCharts();

let ws = null;
let currentSub = null;
let priceData = [];
let cvdData = [];
let isLoadingHistory = false;
let isSyncing = false;

function setStatus(text, ok = true) {
  statusEl.textContent = text;
  statusEl.style.color = ok ? "#8ddc8d" : "#ef7c7c";
}

function toSeriesData(candle) {
  const tsMs = candle.t ?? candle.minute_ts;
  return {
    time: Math.floor(tsMs / 1000),
    open: candle.o ?? candle.open,
    high: candle.h ?? candle.high,
    low: candle.l ?? candle.low,
    close: candle.c ?? candle.close,
    volume: candle.v ?? candle.volume ?? 0,
  };
}

function toVolumeData(bar) {
  return {
    time: bar.time,
    value: bar.volume || 0,
    color: bar.close >= bar.open ? "#2e7d32" : "#c62828",
  };
}

function formatCompact(value) {
  const abs = Math.abs(value);
  if (abs >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(2)}B`;
  if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
  if (abs >= 1_000) return `${(value / 1_000).toFixed(2)}K`;
  return value.toFixed(2);
}

function mergeSeriesData(existing, incoming) {
  const map = new Map();
  existing.forEach((bar) => map.set(bar.time, bar));
  incoming.forEach((bar) => map.set(bar.time, bar));
  return Array.from(map.values()).sort((a, b) => a.time - b.time);
}

function applyPriceData(data) {
  priceData = data.slice();
  priceSeries.setData(priceData);
  volumeSeries.setData(priceData.map(toVolumeData));
}

function applyCvdData(data) {
  cvdData = data.slice();
  cvdSeries.setData(cvdData);
}

function parseIntervalToSec(interval) {
  const trimmed = interval.trim().toLowerCase();
  if (/^\d+$/.test(trimmed)) {
    return parseInt(trimmed, 10) * 60;
  }
  const value = parseInt(trimmed.slice(0, -1), 10);
  const unit = trimmed.slice(-1);
  if (!value) {
    return 60;
  }
  if (unit === "m") return value * 60;
  if (unit === "h") return value * 3600;
  if (unit === "d") return value * 86400;
  return 60;
}

function deriveRestBase(wsUrl) {
  if (!wsUrl) return "";
  try {
    const url = new URL(wsUrl);
    url.protocol = url.protocol === "wss:" ? "https:" : "http:";
    url.pathname = "";
    return url.toString().replace(/\/$/, "");
  } catch {
    return "";
  }
}

function resetSeries() {
  priceData = [];
  cvdData = [];
  priceSeries.setData([]);
  cvdSeries.setData([]);
  volumeSeries.setData([]);
}

async function loadMoreHistory() {
  if (!currentSub || isLoadingHistory || priceData.length === 0 || !currentSub.restBase) return;
  const { symbol, timeframe, snapshotLimit, restBase } = currentSub;
  const intervalSec = parseIntervalToSec(timeframe);
  const earliest = priceData[0].time;
  const endMs = earliest * 1000 - 1;
  const startMs = endMs - intervalSec * 1000 * (snapshotLimit - 1);
  isLoadingHistory = true;
  try {
    const params = new URLSearchParams({
      symbol,
      start_ms: String(startMs),
      end_ms: String(endMs),
      interval: timeframe,
    });
    const [priceResp, cvdResp] = await Promise.all([
      fetch(`${restBase}/candles?${params}`),
      fetch(`${restBase}/cvd?${params}`),
    ]);
    if (priceResp.ok) {
      const priceJson = await priceResp.json();
      const newData = priceJson.map(toSeriesData);
      if (newData.length) {
        priceData = mergeSeriesData(priceData, newData);
        applyPriceData(priceData);
      }
    } else {
      console.warn("Price history fetch failed", await priceResp.text());
    }
    if (cvdResp.ok) {
      const cvdJson = await cvdResp.json();
      const newCvd = cvdJson.map(toSeriesData);
      if (newCvd.length) {
        cvdData = mergeSeriesData(cvdData, newCvd);
        applyCvdData(cvdData);
      }
    } else {
      console.warn("CVD history fetch failed", await cvdResp.text());
    }
  } catch (err) {
    console.warn("History fetch error", err);
  } finally {
    isLoadingHistory = false;
  }
}

function connect() {
  if (ws) {
    ws.close();
  }
  const symbol = symbolInput.value.trim().toUpperCase();
  const timeframe = timeframeInput.value.trim().toLowerCase();
  const snapshotLimit = parseInt(snapshotInput.value, 10) || 200;
  const wsUrl = wsUrlInput.value.trim();
  const restBase = deriveRestBase(wsUrl);
  resetSeries();
  currentSub = { symbol, timeframe, snapshotLimit, restBase };
  setStatus("Connecting...");
  connectBtn.disabled = true;
  disconnectBtn.disabled = false;
  ws = new WebSocket(wsUrl);

  ws.onopen = () => {
    setStatus("Connected");
    ws.send(
      JSON.stringify({
        type: "subscribe",
        symbol,
        timeframe,
        streams: ["price", "cvd"],
        snapshot_limit: snapshotLimit,
      })
    );
  };

  ws.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    if (currentSub && msg.symbol && msg.timeframe) {
      if (msg.symbol !== currentSub.symbol || msg.timeframe !== currentSub.timeframe) {
        return;
      }
    }
    if (msg.type === "snapshot") {
      if (msg.price) {
        applyPriceData(msg.price.map(toSeriesData));
      }
      if (msg.cvd) {
        applyCvdData(msg.cvd.map(toSeriesData));
      }
      return;
    }
    if (msg.type === "update") {
      if (msg.price) {
        const bar = toSeriesData(msg.price);
        if (priceData.length && priceData[priceData.length - 1].time === bar.time) {
          priceData[priceData.length - 1] = bar;
        } else {
          priceData.push(bar);
        }
        priceSeries.update(bar);
        volumeSeries.update(toVolumeData(bar));
      }
      if (msg.cvd) {
        const bar = toSeriesData(msg.cvd);
        if (cvdData.length && cvdData[cvdData.length - 1].time === bar.time) {
          cvdData[cvdData.length - 1] = bar;
        } else {
          cvdData.push(bar);
        }
        cvdSeries.update(bar);
      }
      return;
    }
    if (msg.type === "error") {
      setStatus(`Error: ${msg.message}`, false);
    }
  };

  ws.onclose = () => {
    setStatus("Disconnected", false);
    connectBtn.disabled = false;
    disconnectBtn.disabled = true;
  };

  ws.onerror = () => {
    setStatus("WebSocket error", false);
    connectBtn.disabled = false;
    disconnectBtn.disabled = true;
  };
}

function disconnect() {
  if (ws) {
    try {
      if (currentSub) {
        ws.send(
          JSON.stringify({
            type: "unsubscribe",
            symbol: currentSub.symbol,
            timeframe: currentSub.timeframe,
          })
        );
      }
    } catch {
      // ignore
    }
    ws.close();
    ws = null;
  }
}

connectBtn.addEventListener("click", connect);
disconnectBtn.addEventListener("click", disconnect);

priceChart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
  if (!range || isSyncing) return;
  isSyncing = true;
  cvdChart.timeScale().setVisibleLogicalRange(range);
  isSyncing = false;
  if (range.from < 10) {
    loadMoreHistory();
  }
});

cvdChart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
  if (!range || isSyncing) return;
  isSyncing = true;
  priceChart.timeScale().setVisibleLogicalRange(range);
  isSyncing = false;
});
