import os
import io
import math
import time
import json
import sqlite3
import asyncio
import threading
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import requests
try:
    import ccxt
except Exception:
    ccxt = None
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

START_TIME = time.time()
BINANCE_SPOT = os.getenv("BINANCE_SPOT", "https://api.binance.com")
BINANCE_FUTURES = os.getenv("BINANCE_FUTURES", "https://fapi.binance.com")
BYBIT_API = os.getenv("BYBIT_API", "https://api.bybit.com")
OKX_API = os.getenv("OKX_API", "https://www.okx.com")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "500"))
TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "12"))
PRIMARY_TF = os.getenv("PRIMARY_TF", "1h")
TIMEFRAMES = [x.strip() for x in os.getenv("TIMEFRAMES", "15m,1h,4h,1d").split(",") if x.strip()]
DB_PATH = os.getenv("DATABASE_PATH", "signals.db")
AUTO_SIGNALS_ENABLED = os.getenv("AUTO_SIGNALS_ENABLED", "false").lower() == "true"
AUTO_SIGNAL_CHAT_ID = os.getenv("AUTO_SIGNAL_CHAT_ID")
AUTO_SIGNAL_MIN_CONFIDENCE = int(os.getenv("AUTO_SIGNAL_MIN_CONFIDENCE", "76"))
AUTO_SIGNAL_INTERVAL_MIN = int(os.getenv("AUTO_SIGNAL_INTERVAL_MIN", "15"))
DASHBOARD_PORT = int(os.getenv("PORT", os.getenv("DASHBOARD_PORT", "8080")))
ENABLE_DASHBOARD = os.getenv("ENABLE_DASHBOARD", "true").lower() == "true"
SYMBOLS = {"btc": "BTCUSDT", "eth": "ETHUSDT", "sol": "SOLUSDT", "xrp": "XRPUSDT"}

@dataclass
class Signal:
    symbol: str
    side: str
    confidence: int
    long_probability: int
    short_probability: int
    continuation_probability: int
    entry: float
    stop: float
    take1: float
    take2: float
    take3: float
    rr: float
    text: str
    regime: str
    score: float
    setup_quality: str
    risk_score: int
    expected_move_pct: float
    wave_label: str
    wave_confidence: int
    monte_carlo_survival: int
    walk_forward_winrate: float


SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
    "Connection": "keep-alive",
})

def http_get(url: str, params: dict | None = None, retries: int = 2):
    last_error = None
    for attempt in range(retries + 1):
        try:
            r = SESSION.get(url, params=params or {}, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_error = e
            time.sleep(0.35 * (attempt + 1))
    raise last_error


def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        confidence INTEGER,
        long_probability INTEGER,
        short_probability INTEGER,
        continuation_probability INTEGER,
        entry REAL,
        stop REAL,
        take1 REAL,
        take2 REAL,
        take3 REAL,
        rr REAL,
        regime TEXT,
        score REAL,
        setup_quality TEXT,
        risk_score INTEGER,
        expected_move_pct REAL,
        wave_label TEXT,
        wave_confidence INTEGER,
        monte_carlo_survival INTEGER,
        walk_forward_winrate REAL,
        result TEXT DEFAULT 'open',
        meta_json TEXT
    )
    """)
    con.commit(); con.close()


def save_signal(signal: Signal, meta: dict):
    init_db()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
    INSERT INTO signals (ts,symbol,side,confidence,long_probability,short_probability,continuation_probability,entry,stop,take1,take2,take3,rr,regime,score,setup_quality,risk_score,expected_move_pct,wave_label,wave_confidence,monte_carlo_survival,walk_forward_winrate,meta_json)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        datetime.now(timezone.utc).isoformat(), signal.symbol, signal.side, signal.confidence,
        signal.long_probability, signal.short_probability, signal.continuation_probability,
        signal.entry, signal.stop, signal.take1, signal.take2, signal.take3, signal.rr,
        signal.regime, signal.score, signal.setup_quality, signal.risk_score,
        signal.expected_move_pct, signal.wave_label, signal.wave_confidence,
        signal.monte_carlo_survival, signal.walk_forward_winrate,
        json.dumps(meta, default=str)[:8000]
    ))
    con.commit(); con.close()


def db_stats(limit=20):
    init_db()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    total = cur.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    rows = cur.execute("SELECT ts,symbol,side,confidence,long_probability,short_probability,regime,setup_quality,rr FROM signals ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    by_symbol = cur.execute("SELECT symbol, COUNT(*), AVG(confidence), AVG(rr) FROM signals GROUP BY symbol").fetchall()
    con.close()
    return {"total": total, "latest": rows, "by_symbol": by_symbol}


def _parse_klines(raw) -> pd.DataFrame:
    """Parse Binance kline payload."""
    cols = ["open_time", "open", "high", "low", "close", "volume", "close_time", "qav", "trades", "tbav", "tqav", "ignore"]
    df = pd.DataFrame(raw, columns=cols)
    for c in ["open", "high", "low", "close", "volume", "qav", "tbav", "tqav"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["taker_buy_vol"] = pd.to_numeric(df["tbav"], errors="coerce")
    return df[["time", "open", "high", "low", "close", "volume", "taker_buy_vol", "trades"]].dropna().sort_values("time").reset_index(drop=True)


def _okx_inst_id(symbol: str) -> str:
    base = symbol.replace("USDT", "")
    return f"{base}-USDT"


def _okx_bar(interval: str) -> str:
    return {"15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}.get(interval, interval)


def _bybit_interval(interval: str) -> str:
    return {"15m": "15", "1h": "60", "4h": "240", "1d": "D"}.get(interval, interval)


def _parse_bybit_klines(payload: dict) -> pd.DataFrame:
    rows = payload.get("result", {}).get("list", [])
    if not rows:
        raise ValueError("Bybit returned empty candles")
    df = pd.DataFrame(rows, columns=["open_time", "open", "high", "low", "close", "volume", "turnover"])
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["time"] = pd.to_datetime(pd.to_numeric(df["open_time"], errors="coerce"), unit="ms", utc=True)
    # Bybit public candles don't include taker-buy volume in this endpoint.
    # Approximate delta direction from candle body so CVD module remains usable.
    direction = np.where(df["close"] >= df["open"], 0.58, 0.42)
    df["taker_buy_vol"] = df["volume"] * direction
    df["trades"] = 0
    return df[["time", "open", "high", "low", "close", "volume", "taker_buy_vol", "trades"]].dropna().sort_values("time").reset_index(drop=True)


def _parse_okx_klines(payload: dict) -> pd.DataFrame:
    rows = payload.get("data", [])
    if not rows:
        raise ValueError("OKX returned empty candles")
    df = pd.DataFrame(rows, columns=["open_time", "open", "high", "low", "close", "volume", "vol_ccy", "vol_quote", "confirm"])
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["time"] = pd.to_datetime(pd.to_numeric(df["open_time"], errors="coerce"), unit="ms", utc=True)
    direction = np.where(df["close"] >= df["open"], 0.58, 0.42)
    df["taker_buy_vol"] = df["volume"] * direction
    df["trades"] = 0
    return df[["time", "open", "high", "low", "close", "volume", "taker_buy_vol", "trades"]].dropna().sort_values("time").reset_index(drop=True)



def _symbol_to_ccxt(symbol: str) -> str:
    base = symbol.replace("USDT", "")
    return f"{base}/USDT"


def _parse_ccxt_ohlcv(rows, source: str) -> pd.DataFrame:
    if not rows:
        raise ValueError(f"{source} returned empty candles")
    df = pd.DataFrame(rows, columns=["open_time", "open", "high", "low", "close", "volume"])
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["time"] = pd.to_datetime(pd.to_numeric(df["open_time"], errors="coerce"), unit="ms", utc=True)
    # Most public OHLCV endpoints do not expose true taker-buy volume.
    # Approximation keeps CVD module alive but labels source as fallback.
    body = (df["close"] - df["open"]) / (df["high"] - df["low"]).replace(0, np.nan)
    direction = (0.50 + body.clip(-1, 1).fillna(0) * 0.18).clip(0.32, 0.68)
    df["taker_buy_vol"] = df["volume"] * direction
    df["trades"] = 0
    out = df[["time", "open", "high", "low", "close", "volume", "taker_buy_vol", "trades"]].dropna().sort_values("time").reset_index(drop=True)
    out.attrs["market_source"] = source
    return out


def fetch_klines_ccxt(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    if ccxt is None:
        raise RuntimeError("ccxt is not installed")
    ccxt_symbol = _symbol_to_ccxt(symbol)
    # Public exchanges with decent Railway/cloud reliability. enableRateLimit is important on Railway.
    exchange_specs = [
        ("binanceusdm", {"options": {"defaultType": "future"}}),
        ("binance", {"options": {"defaultType": "spot"}}),
        ("bybit", {"options": {"defaultType": "spot"}}),
        ("okx", {"options": {"defaultType": "spot"}}),
        ("kraken", {}),
        ("coinbase", {}),
    ]
    errors = []
    for ex_id, extra in exchange_specs:
        try:
            klass = getattr(ccxt, ex_id)
            ex = klass({
                "enableRateLimit": True,
                "timeout": TIMEOUT * 1000,
                "headers": {"User-Agent": SESSION.headers["User-Agent"]},
                **extra,
            })
            # Avoid full market loading when possible; if symbol check fails, try anyway.
            symbol_to_use = ccxt_symbol
            if ex_id == "coinbase" and symbol.startswith("XRP"):
                symbol_to_use = "XRP/USD"
            elif ex_id == "coinbase":
                symbol_to_use = symbol.replace("USDT", "/USD")
            rows = ex.fetch_ohlcv(symbol_to_use, timeframe=interval, limit=min(limit, 500))
            df = _parse_ccxt_ohlcv(rows, f"ccxt_{ex_id}")
            if len(df) >= 80:
                return df
            errors.append(f"{ex_id}: too_few_candles")
        except Exception as e:
            errors.append(f"{ex_id}: {type(e).__name__}")
            print(f"[WARN] ccxt {ex_id} failed for {symbol} {interval}: {e}")
    raise RuntimeError("CCXT providers failed: " + "; ".join(errors))


def _cryptocompare_symbol(symbol: str) -> str:
    return symbol.replace("USDT", "")


def fetch_cryptocompare(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    fsym = _cryptocompare_symbol(symbol)
    if interval == "15m":
        endpoint = "histominute"; params = {"fsym": fsym, "tsym": "USDT", "aggregate": 15, "limit": min(limit, 2000)}
    elif interval == "1h":
        endpoint = "histohour"; params = {"fsym": fsym, "tsym": "USDT", "limit": min(limit, 2000)}
    elif interval == "4h":
        endpoint = "histohour"; params = {"fsym": fsym, "tsym": "USDT", "aggregate": 4, "limit": min(limit, 2000)}
    else:
        endpoint = "histoday"; params = {"fsym": fsym, "tsym": "USDT", "limit": min(limit, 2000)}
    data = http_get(f"https://min-api.cryptocompare.com/data/v2/{endpoint}", params)
    rows = data.get("Data", {}).get("Data", [])
    if not rows:
        raise ValueError("CryptoCompare returned empty candles")
    df = pd.DataFrame(rows)
    df = df.rename(columns={"time": "open_time", "volumefrom": "volume"})
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["time"] = pd.to_datetime(pd.to_numeric(df["open_time"], errors="coerce"), unit="s", utc=True)
    body = (df["close"] - df["open"]) / (df["high"] - df["low"]).replace(0, np.nan)
    direction = (0.50 + body.clip(-1, 1).fillna(0) * 0.18).clip(0.32, 0.68)
    df["taker_buy_vol"] = df["volume"] * direction
    df["trades"] = 0
    out = df[["time", "open", "high", "low", "close", "volume", "taker_buy_vol", "trades"]].dropna().sort_values("time").reset_index(drop=True)
    out.attrs["market_source"] = "cryptocompare_fallback"
    return out

def fetch_klines(symbol: str, interval: str = PRIMARY_TF, limit: int = DEFAULT_LIMIT, futures: bool = True) -> pd.DataFrame:
    """Railway-safe candle fetcher.

    v9 uses layered networking:
    1) CCXT exchanges: Binance Futures/Spot, Bybit, OKX, Kraken, Coinbase
    2) Direct REST fallbacks: Binance, Bybit, OKX
    3) CryptoCompare public market data

    This avoids the common Railway/Binance HTTP 451 problem and also handles SSL/timeouts better. If every market source is blocked, v9 returns synthetic OFFLINE candles so the bot does not crash.
    """
    errors = []
    # First: CCXT. It handles many exchange quirks better than raw requests.
    try:
        return fetch_klines_ccxt(symbol, interval, limit)
    except Exception as e:
        errors.append(f"CCXT: {type(e).__name__}")
        print(f"[WARN] CCXT layer failed for {symbol} {interval}: {e}")

    # Second: raw REST fallbacks, kept for small installs where ccxt is unavailable.
    if futures:
        try:
            raw = http_get(f"{BINANCE_FUTURES}/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})
            df = _parse_klines(raw); df.attrs["market_source"] = "binance_futures_rest"; return df
        except Exception as e:
            errors.append(f"Binance Futures REST: {type(e).__name__}")
            print(f"[WARN] Binance Futures REST failed for {symbol} {interval}: {e}")
    try:
        raw = http_get(f"{BINANCE_SPOT}/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})
        df = _parse_klines(raw); df.attrs["market_source"] = "binance_spot_rest"; return df
    except Exception as e:
        errors.append(f"Binance Spot REST: {type(e).__name__}")
        print(f"[WARN] Binance Spot REST failed for {symbol} {interval}: {e}")
    try:
        raw = http_get(f"{BYBIT_API}/v5/market/kline", {"category": "spot", "symbol": symbol, "interval": _bybit_interval(interval), "limit": min(limit, 1000)})
        df = _parse_bybit_klines(raw); df.attrs["market_source"] = "bybit_spot_rest"; return df
    except Exception as e:
        errors.append(f"Bybit REST: {type(e).__name__}")
        print(f"[WARN] Bybit REST failed for {symbol} {interval}: {e}")
    try:
        raw = http_get(f"{OKX_API}/api/v5/market/candles", {"instId": _okx_inst_id(symbol), "bar": _okx_bar(interval), "limit": min(limit, 300)})
        df = _parse_okx_klines(raw); df.attrs["market_source"] = "okx_spot_rest"; return df
    except Exception as e:
        errors.append(f"OKX REST: {type(e).__name__}")
        print(f"[WARN] OKX REST failed for {symbol} {interval}: {e}")

    # Third: market-data aggregator fallback.
    try:
        return fetch_cryptocompare(symbol, interval, limit)
    except Exception as e:
        errors.append(f"CryptoCompare: {type(e).__name__}")
        print(f"[WARN] CryptoCompare failed for {symbol} {interval}: {e}")

    # Fourth: Yahoo Finance chart API. Usually works from Railway even when crypto exchanges block cloud IPs.
    try:
        return fetch_yahoo(symbol, interval, limit)
    except Exception as e:
        errors.append(f"Yahoo: {type(e).__name__}")
        print(f"[WARN] Yahoo Finance failed for {symbol} {interval}: {e}")

    # Last resort: do not crash the bot. This is clearly labeled in the report as OFFLINE, not real market data.
    print(f"[ERROR] All market providers failed for {symbol} {interval}: {'; '.join(errors)}")
    return generate_synthetic_ohlcv(symbol, interval, limit)



def _yahoo_symbol(symbol: str) -> str:
    base = symbol.replace("USDT", "")
    return f"{base}-USD"


def _yahoo_range_interval(interval: str, limit: int):
    # Yahoo supports 15m/1h intervals for recent ranges and 1d for long ranges.
    if interval == "15m":
        return "15m", "5d"
    if interval == "1h":
        return "1h", "30d"
    if interval == "4h":
        # Yahoo has no 4h interval; fetch 1h and resample below.
        return "1h", "60d"
    return "1d", "400d"


def fetch_yahoo(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    """Railway-friendly public fallback via Yahoo Finance chart API."""
    ysym = _yahoo_symbol(symbol)
    yinterval, yrange = _yahoo_range_interval(interval, limit)
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ysym}"
    data = http_get(url, {"interval": yinterval, "range": yrange, "includePrePost": "false"})
    result = (data.get("chart", {}).get("result") or [None])[0]
    if not result:
        raise ValueError("Yahoo returned no chart result")
    ts = result.get("timestamp") or []
    q = (result.get("indicators", {}).get("quote") or [{}])[0]
    if not ts or not q:
        raise ValueError("Yahoo returned empty candles")
    df = pd.DataFrame({
        "time": pd.to_datetime(ts, unit="s", utc=True),
        "open": q.get("open"),
        "high": q.get("high"),
        "low": q.get("low"),
        "close": q.get("close"),
        "volume": q.get("volume"),
    })
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"]).sort_values("time")
    df["volume"] = df["volume"].fillna(0)
    if interval == "4h":
        df = df.set_index("time").resample("4h").agg({
            "open":"first", "high":"max", "low":"min", "close":"last", "volume":"sum"
        }).dropna().reset_index()
    body = (df["close"] - df["open"]) / (df["high"] - df["low"]).replace(0, np.nan)
    direction = (0.50 + body.clip(-1, 1).fillna(0) * 0.18).clip(0.32, 0.68)
    df["taker_buy_vol"] = df["volume"] * direction
    df["trades"] = 0
    out = df[["time", "open", "high", "low", "close", "volume", "taker_buy_vol", "trades"]].tail(limit).reset_index(drop=True)
    if len(out) < 60:
        raise ValueError("Yahoo returned too few candles")
    out.attrs["market_source"] = "yahoo_finance_fallback"
    return out


def generate_synthetic_ohlcv(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    """Last-resort no-crash mode. Not real market data; used only when every provider is blocked."""
    import hashlib
    seed = int(hashlib.sha256(f"{symbol}-{interval}".encode()).hexdigest()[:8], 16)
    rng = np.random.default_rng(seed)
    base_prices = {"BTCUSDT": 65000, "ETHUSDT": 3200, "SOLUSDT": 150, "XRPUSDT": 0.55}
    start = float(base_prices.get(symbol, 100))
    step = {"15m":"15min", "1h":"1h", "4h":"4h", "1d":"1d"}.get(interval, "15min")
    times = pd.date_range(end=pd.Timestamp.utcnow().floor("min"), periods=limit, freq=step)
    returns = rng.normal(0, 0.003 if interval != "1d" else 0.018, limit)
    close = start * np.exp(np.cumsum(returns))
    open_ = np.r_[close[0], close[:-1]]
    spread = np.maximum(np.abs(close-open_), close * rng.uniform(0.001, 0.007, limit))
    high = np.maximum(open_, close) + spread * rng.uniform(0.4, 1.2, limit)
    low = np.minimum(open_, close) - spread * rng.uniform(0.4, 1.2, limit)
    volume = rng.uniform(1000, 10000, limit) * (start / max(close.mean(), 1))
    df = pd.DataFrame({"time":times, "open":open_, "high":high, "low":low, "close":close, "volume":volume})
    body = (df["close"] - df["open"]) / (df["high"] - df["low"]).replace(0, np.nan)
    direction = (0.50 + body.clip(-1, 1).fillna(0) * 0.18).clip(0.32, 0.68)
    df["taker_buy_vol"] = df["volume"] * direction
    df["trades"] = 0
    df.attrs["market_source"] = "OFFLINE_SYNTHETIC_NOT_REAL_MARKET_DATA"
    return df[["time", "open", "high", "low", "close", "volume", "taker_buy_vol", "trades"]]

def fetch_futures_context(symbol: str) -> dict:
    ctx = {
        "funding": None, "open_interest": None, "depth_imbalance": None,
        "market_source": "binance_futures",
        "glassnode": "not_configured", "coinglass": "not_configured"
    }
    try:
        prem = http_get(f"{BINANCE_FUTURES}/fapi/v1/premiumIndex", {"symbol": symbol})
        ctx["funding"] = float(prem.get("lastFundingRate", 0))
    except Exception as e:
        ctx["funding_error"] = str(e)
        ctx["market_source"] = "binance_spot_fallback"
    try:
        oi = http_get(f"{BINANCE_FUTURES}/fapi/v1/openInterest", {"symbol": symbol})
        ctx["open_interest"] = float(oi.get("openInterest", 0))
    except Exception as e:
        ctx["oi_error"] = str(e)
        ctx["market_source"] = "binance_spot_fallback"
    # Orderbook imbalance: first Futures, then Spot fallback.
    try:
        depth = http_get(f"{BINANCE_FUTURES}/fapi/v1/depth", {"symbol": symbol, "limit": 100})
    except Exception as e:
        ctx["futures_depth_error"] = str(e)
        ctx["market_source"] = "binance_spot_fallback"
        try:
            depth = http_get(f"{BINANCE_SPOT}/api/v3/depth", {"symbol": symbol, "limit": 100})
        except Exception as e2:
            ctx["spot_depth_error"] = type(e2).__name__
            try:
                by = http_get(f"{BYBIT_API}/v5/market/orderbook", {"category": "spot", "symbol": symbol, "limit": 50})
                r = by.get("result", {})
                depth = {"bids": r.get("b", []), "asks": r.get("a", [])}
                ctx["market_source"] = "bybit_spot_fallback"
            except Exception as e3:
                ctx["bybit_depth_error"] = type(e3).__name__
                try:
                    ok = http_get(f"{OKX_API}/api/v5/market/books", {"instId": _okx_inst_id(symbol), "sz": 50})
                    r = (ok.get("data") or [{}])[0]
                    depth = {"bids": r.get("bids", []), "asks": r.get("asks", [])}
                    ctx["market_source"] = "okx_spot_fallback"
                except Exception as e4:
                    ctx["depth_error"] = type(e4).__name__
                    depth = {"bids": [], "asks": []}
    try:
        bid_qty = sum(float(x[1]) for x in depth.get("bids", [])[:50])
        ask_qty = sum(float(x[1]) for x in depth.get("asks", [])[:50])
        ctx["depth_imbalance"] = (bid_qty - ask_qty) / max(bid_qty + ask_qty, 1e-9)
    except Exception as e:
        ctx["depth_calc_error"] = str(e)
    if os.getenv("GLASSNODE_API_KEY"): ctx["glassnode"] = "api_key_present_optional_hook"
    if os.getenv("COINGLASS_API_KEY"): ctx["coinglass"] = "api_key_present_optional_hook"
    return ctx


def ema(s, p): return s.ewm(span=p, adjust=False).mean()

def rsi(close, period=14):
    d = close.diff(); gain = d.clip(lower=0).rolling(period).mean(); loss = (-d.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def atr(df, period=14):
    tr = pd.concat([(df.high-df.low), (df.high-df.close.shift()).abs(), (df.low-df.close.shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def macd(close):
    m = ema(close, 12) - ema(close, 26); sig = ema(m, 9); return m, sig, m-sig

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for p in [9, 20, 50, 100, 200]: df[f"ema{p}"] = ema(df.close, p)
    df["rsi"] = rsi(df.close); df["atr"] = atr(df); df["macd"], df["macd_signal"], df["macd_hist"] = macd(df.close)
    taker_delta = (df.taker_buy_vol - (df.volume - df.taker_buy_vol)).fillna(0)
    df["cvd"] = pd.Series(taker_delta, index=df.index).cumsum()
    df["vol_ma"] = df.volume.rolling(30).mean(); df["ret"] = df.close.pct_change(); df["realized_vol"] = df.ret.rolling(48).std() * np.sqrt(48)
    return df


def pivots(df, window=5):
    highs, lows = [], []
    for i in range(window, len(df)-window):
        if df.high.iloc[i] == df.high.iloc[i-window:i+window+1].max(): highs.append((i, float(df.high.iloc[i])))
        if df.low.iloc[i] == df.low.iloc[i-window:i+window+1].min(): lows.append((i, float(df.low.iloc[i])))
    return highs[-16:], lows[-16:]

def fit_line(points, n):
    if len(points) < 2: return None
    x = np.array([p[0] for p in points], dtype=float); y = np.array([p[1] for p in points], dtype=float)
    a, b = np.polyfit(x, y, 1); return a, b, float(a*(n-1)+b)

def fib_levels(df):
    recent = df.tail(220); hi = float(recent.high.max()); lo = float(recent.low.min()); diff = hi-lo
    return hi, lo, {"0.236": hi-diff*.236, "0.382": hi-diff*.382, "0.500": hi-diff*.5, "0.618": hi-diff*.618, "0.786": hi-diff*.786, "1.272 ext": hi+diff*.272, "1.618 ext": hi+diff*.618}

def market_structure(df):
    highs, lows = pivots(df, 4)
    bos_up = len(highs) >= 2 and df.close.iloc[-1] > highs[-1][1]
    bos_down = len(lows) >= 2 and df.close.iloc[-1] < lows[-1][1]
    liq_high = max([h[1] for h in highs[-3:]], default=float(df.high.tail(60).max()))
    liq_low = min([l[1] for l in lows[-3:]], default=float(df.low.tail(60).min()))
    sweep_high = df.high.iloc[-1] > liq_high and df.close.iloc[-1] < liq_high
    sweep_low = df.low.iloc[-1] < liq_low and df.close.iloc[-1] > liq_low
    choch_up = len(lows) >= 2 and len(highs) >= 2 and lows[-1][1] > lows[-2][1] and df.close.iloc[-1] > highs[-1][1]
    choch_down = len(lows) >= 2 and len(highs) >= 2 and highs[-1][1] < highs[-2][1] and df.close.iloc[-1] < lows[-1][1]
    return {"highs": highs, "lows": lows, "bos_up": bos_up, "bos_down": bos_down, "choch_up": choch_up, "choch_down": choch_down, "liq_high": liq_high, "liq_low": liq_low, "sweep_high": sweep_high, "sweep_low": sweep_low}


def elliott_wave(df: pd.DataFrame) -> dict:
    d = df.tail(260).reset_index(drop=True)
    highs, lows = pivots(d, 5)
    swings = sorted([(i, p, "H") for i, p in highs] + [(i, p, "L") for i, p in lows], key=lambda x: x[0])[-12:]
    price = float(d.close.iloc[-1])
    if len(swings) < 5:
        return {"bias": 0, "label": "not_enough_swings", "confidence": 35, "points": swings, "target": price, "invalidation": price}
    pts = swings[-6:]
    first, last = pts[0], pts[-1]
    direction = 1 if last[1] > first[1] else -1
    highs_seq = [p for _, p, t in pts if t == "H"]
    lows_seq = [p for _, p, t in pts if t == "L"]
    impulse_quality = 0
    if direction > 0:
        if len(highs_seq) >= 2 and highs_seq[-1] > highs_seq[0]: impulse_quality += 25
        if len(lows_seq) >= 2 and lows_seq[-1] > lows_seq[0]: impulse_quality += 25
        if price > np.mean([p for _, p, _ in pts]): impulse_quality += 15
        wave_label = "Bullish impulse / Wave 3-5 area" if impulse_quality >= 45 else "Bullish ABC recovery"
        target = price + abs(max(highs_seq or [price]) - min(lows_seq or [price])) * 0.618
        invalidation = min(lows_seq[-2:] or [float(d.low.tail(40).min())])
        bias = min(1.0, impulse_quality / 70)
    else:
        if len(highs_seq) >= 2 and highs_seq[-1] < highs_seq[0]: impulse_quality += 25
        if len(lows_seq) >= 2 and lows_seq[-1] < lows_seq[0]: impulse_quality += 25
        if price < np.mean([p for _, p, _ in pts]): impulse_quality += 15
        wave_label = "Bearish impulse / Wave 3-5 area" if impulse_quality >= 45 else "Bearish ABC correction"
        target = price - abs(max(highs_seq or [price]) - min(lows_seq or [price])) * 0.618
        invalidation = max(highs_seq[-2:] or [float(d.high.tail(40).max())])
        bias = -min(1.0, impulse_quality / 70)
    return {"bias": bias, "label": wave_label, "confidence": int(max(35, min(86, 40 + impulse_quality))), "points": pts, "target": float(target), "invalidation": float(invalidation)}


def volume_profile(df: pd.DataFrame, bins: int = 48, lookback: int = 220) -> dict:
    d = df.tail(lookback).dropna().copy()
    if d.empty: return {"poc": np.nan, "vah": np.nan, "val": np.nan, "hvn": [], "lvn": [], "bins": []}
    lo, hi = float(d.low.min()), float(d.high.max())
    edges = np.linspace(lo, hi, bins + 1); mids = (edges[:-1] + edges[1:]) / 2; vol = np.zeros(bins)
    typical = (d.high + d.low + d.close) / 3
    idx = np.clip(np.digitize(typical, edges) - 1, 0, bins - 1)
    for i, v in zip(idx, d.volume): vol[int(i)] += float(v)
    total = max(float(vol.sum()), 1e-9); poc_i = int(np.argmax(vol)); order = np.argsort(vol)[::-1]
    included, cum = [], 0.0
    for i in order:
        included.append(int(i)); cum += vol[i]
        if cum / total >= 0.70: break
    val, vah = float(mids[min(included)]), float(mids[max(included)])
    return {"poc": float(mids[poc_i]), "vah": vah, "val": val, "hvn": [float(mids[i]) for i in order[:5] if vol[i] > 0], "lvn": [float(mids[i]) for i in np.argsort(vol)[:5] if vol[i] > 0], "bins": [(float(mids[i]), float(vol[i])) for i in range(bins)]}


def liquidity_heatmap(df: pd.DataFrame, lookback: int = 180) -> dict:
    d = df.tail(lookback).dropna().copy(); price = float(d.close.iloc[-1])
    atr_v = max(float(d.atr.iloc[-1]) if pd.notna(d.atr.iloc[-1]) else price * 0.01, price * 0.003)
    highs, lows = pivots(d.reset_index(drop=True), 3); levels = []
    for _, lvl in highs[-8:]:
        touches = int((abs(d.high - lvl) <= atr_v * 0.35).sum()); dist = abs(lvl - price) / max(atr_v, 1e-9)
        levels.append({"side": "above", "level": float(lvl), "strength": float(touches * 12 + max(0, 20 - dist * 2))})
    for _, lvl in lows[-8:]:
        touches = int((abs(d.low - lvl) <= atr_v * 0.35).sum()); dist = abs(lvl - price) / max(atr_v, 1e-9)
        levels.append({"side": "below", "level": float(lvl), "strength": float(touches * 12 + max(0, 20 - dist * 2))})
    above = sorted([x for x in levels if x["side"] == "above"], key=lambda x: x["strength"], reverse=True)[:5]
    below = sorted([x for x in levels if x["side"] == "below"], key=lambda x: x["strength"], reverse=True)[:5]
    return {"above": above, "below": below, "magnet_up": above[0]["level"] if above else float(d.high.tail(80).max()), "magnet_down": below[0]["level"] if below else float(d.low.tail(80).min())}


def orderflow_advanced(df: pd.DataFrame) -> dict:
    d = df.tail(120).dropna().copy(); delta = d.taker_buy_vol - (d.volume - d.taker_buy_vol); cvd = delta.cumsum()
    recent_delta = float(delta.tail(24).sum()); delta_strength = recent_delta / max(float(d.volume.tail(24).sum()), 1e-9)
    price_change = float(d.close.iloc[-1] - d.close.iloc[-24]) if len(d) >= 24 else 0.0
    cvd_change = float(cvd.iloc[-1] - cvd.iloc[-24]) if len(cvd) >= 24 else float(cvd.iloc[-1])
    bullish_div = price_change < 0 and cvd_change > 0; bearish_div = price_change > 0 and cvd_change < 0
    absorption = (abs(price_change) < max(float(d.atr.iloc[-1]), d.close.iloc[-1]*0.003) * .55) and abs(delta_strength) > .08
    dominance = "buyers" if delta_strength > .03 else "sellers" if delta_strength < -.03 else "balanced"
    return {"delta_strength": float(delta_strength), "recent_delta": recent_delta, "bullish_div": bool(bullish_div), "bearish_div": bool(bearish_div), "absorption": bool(absorption), "dominance": dominance}


def timeframe_score(df):
    last, prev = df.iloc[-1], df.iloc[-2]; s = 0.0
    if last.ema20 > last.ema50 > last.ema200: s += 2
    elif last.ema20 < last.ema50 < last.ema200: s -= 2
    elif last.ema20 > last.ema50: s += 1
    elif last.ema20 < last.ema50: s -= 1
    if last.macd_hist > 0 and last.macd_hist > prev.macd_hist: s += 1
    if last.macd_hist < 0 and last.macd_hist < prev.macd_hist: s -= 1
    if last.rsi > 55: s += .7
    if last.rsi < 45: s -= .7
    return s


def regime_ai(df: pd.DataFrame, mtf_scores: dict) -> dict:
    d = df.dropna().tail(220); last = d.iloc[-1]
    ema_spread = abs(float(last.ema20 - last.ema100)) / max(float(last.atr), 1e-9)
    vol_rank = float(d.realized_vol.rank(pct=True).iloc[-1]) if d.realized_vol.notna().sum() > 30 else 0.5
    mtf_abs = abs(sum(mtf_scores.values()))
    if ema_spread > 2.2 and mtf_abs > 2.0: regime = "trend"
    elif vol_rank > 0.80: regime = "high_volatility"
    else: regime = "range"
    weights = {"trend": {"trend": 1.35, "mean_reversion": .65, "orderflow": 1.05, "liquidity": .95}, "range": {"trend": .70, "mean_reversion": 1.30, "orderflow": 1.00, "liquidity": 1.20}, "high_volatility": {"trend": .85, "mean_reversion": .75, "orderflow": 1.25, "liquidity": 1.35}}[regime]
    return {"regime": regime, "vol_rank": vol_rank, "ema_spread_atr": ema_spread, "weights": weights}


def backtest_quality(df, direction):
    d = df.dropna().copy().tail(260)
    if len(d) < 80: return 0.5, 0
    wins = total = 0; avg_rr = []
    for i in range(35, len(d)-10):
        row = d.iloc[i]; future = d.iloc[i+1:i+10]; a = max(float(row.atr), float(row.close)*0.004)
        if direction == "LONG":
            tp, sl = row.close + 1.25*a, row.close - 1.0*a
            hit_tp = future.index[future.high >= tp].min() if (future.high >= tp).any() else None
            hit_sl = future.index[future.low <= sl].min() if (future.low <= sl).any() else None
        else:
            tp, sl = row.close - 1.25*a, row.close + 1.0*a
            hit_tp = future.index[future.low <= tp].min() if (future.low <= tp).any() else None
            hit_sl = future.index[future.high >= sl].min() if (future.high >= sl).any() else None
        if hit_tp is not None or hit_sl is not None:
            total += 1; win = hit_sl is None or (hit_tp is not None and hit_tp < hit_sl); wins += int(win); avg_rr.append(1.25 if win else -1.0)
    return (wins / total if total else 0.5), total


def walk_forward_validation(df, direction, folds=4):
    d = df.dropna().tail(360)
    if len(d) < 160: return {"winrate": 0.50, "folds": 0, "stability": 50}
    chunks = np.array_split(d, folds); wrs = []
    for ch in chunks:
        wr, trades = backtest_quality(ch, direction)
        if trades > 8: wrs.append(wr)
    if not wrs: return {"winrate": 0.50, "folds": 0, "stability": 50}
    stability = int(max(0, min(100, 100 - np.std(wrs) * 180)))
    return {"winrate": float(np.mean(wrs)), "folds": len(wrs), "stability": stability}


def monte_carlo_risk(df, side, entry, stop, take2, trials=500):
    d = df.dropna().tail(220); rets = d.close.pct_change().dropna().values
    if len(rets) < 40: return {"survival": 50, "median_move_pct": 0.0, "risk_of_stop": 50}
    horizon = 16; hit_tp = hit_sl = 0; terminal = []
    for _ in range(trials):
        sample = np.random.choice(rets, horizon, replace=True)
        path = entry * np.cumprod(1 + sample)
        if side == "LONG":
            tp_hit = np.any(path >= take2); sl_hit = np.any(path <= stop)
        else:
            tp_hit = np.any(path <= take2); sl_hit = np.any(path >= stop)
        hit_tp += int(tp_hit and not sl_hit); hit_sl += int(sl_hit); terminal.append((path[-1] - entry) / entry * 100 * (1 if side == "LONG" else -1))
    survival = int(max(1, min(99, 100 - hit_sl / trials * 100)))
    return {"survival": survival, "median_move_pct": float(np.median(terminal)), "risk_of_stop": int(hit_sl / trials * 100)}


def ai_prediction_score(df, mtf_scores):
    last = df.iloc[-1]
    features = np.array([
        np.tanh((last.ema20-last.ema50)/max(last.atr, 1e-9)),
        np.tanh((last.ema50-last.ema200)/max(last.atr, 1e-9)),
        (last.rsi-50)/50,
        np.tanh(last.macd_hist/max(abs(df.macd_hist.tail(100)).median(), 1e-9)),
        np.tanh((df.cvd.iloc[-1]-df.cvd.iloc[-30])/max(abs(df.cvd.diff().tail(100)).sum(), 1e-9)),
        np.tanh(np.mean(mtf_scores)/2.5) if mtf_scores else 0,
    ])
    weights = np.array([0.22, 0.18, 0.14, 0.16, 0.13, 0.17])
    raw = float(np.dot(features, weights)); prob_up = 1/(1+math.exp(-3*raw))
    return prob_up, features


def setup_quality(confidence, rr, wf_winrate, mc_survival):
    grade_score = confidence * 0.45 + min(rr, 4) * 8 + wf_winrate * 25 + mc_survival * 0.15
    if grade_score >= 88: return "A+"
    if grade_score >= 78: return "A"
    if grade_score >= 66: return "B"
    return "C"


def analyze(symbol: str) -> tuple[Signal, dict]:
    frames = {}
    fetch_errors = []
    for tf in TIMEFRAMES:
        try:
            frames[tf] = add_indicators(fetch_klines(symbol, tf, DEFAULT_LIMIT, True))
        except Exception as e:
            fetch_errors.append(f"{tf}: {type(e).__name__}")
            print(f"[WARN] timeframe fetch failed {symbol} {tf}: {e}")
    if not frames:
        raise RuntimeError("Не удалось получить рыночные данные ни по одному таймфрейму: " + "; ".join(fetch_errors))
    df = frames[PRIMARY_TF] if PRIMARY_TF in frames else frames[TIMEFRAMES[0]]
    ctx = fetch_futures_context(symbol); ctx["candle_source"] = df.attrs.get("market_source", "unknown"); ms = market_structure(df); vp = volume_profile(df); heat = liquidity_heatmap(df); oflow = orderflow_advanced(df); wave = elliott_wave(df)
    highs, lows = ms["highs"], ms["lows"]; res_line, sup_line = fit_line(highs[-5:], len(df)), fit_line(lows[-5:], len(df)); hi, lo, fibs = fib_levels(df)
    last, prev = df.iloc[-1], df.iloc[-2]; price = float(last.close); atr_v = max(float(last.atr) if pd.notna(last.atr) else price*.01, price*.003)
    mtf = {tf: timeframe_score(frames[tf].dropna()) for tf in frames if len(frames[tf].dropna()) > 220}; reg = regime_ai(df, mtf); prob_up, features = ai_prediction_score(df.dropna(), list(mtf.values()))
    score = (prob_up - 0.5) * 70; reasons = [f"AI prediction engine: вероятность роста {prob_up*100:.1f}%", f"Multi-TF score: {sum(mtf.values()):+.2f} ({', '.join([k+':'+format(v, '+.1f') for k,v in mtf.items()])})"]

    if last.ema20 > last.ema50 > last.ema200: score += 18; reasons.append("EMA 20>50>200: трендовый бычий режим")
    elif last.ema20 < last.ema50 < last.ema200: score -= 18; reasons.append("EMA 20<50<200: трендовый медвежий режим")
    if last.rsi > 72: score -= 8; reasons.append("RSI перекуплен: повышен риск отката")
    elif last.rsi < 28: score += 8; reasons.append("RSI перепродан: возможен отскок")
    if last.macd_hist > 0 and last.macd_hist > prev.macd_hist: score += 8; reasons.append("MACD histogram усиливается вверх")
    elif last.macd_hist < 0 and last.macd_hist < prev.macd_hist: score -= 8; reasons.append("MACD histogram усиливается вниз")
    if ms["bos_up"] or ms["choch_up"]: score += 10; reasons.append("Smart Money: BOS/CHOCH вверх")
    if ms["bos_down"] or ms["choch_down"]: score -= 10; reasons.append("Smart Money: BOS/CHOCH вниз")
    if ms["sweep_low"]: score += 8; reasons.append("Liquidity sweep снизу: возможный лонг-отскок")
    if ms["sweep_high"]: score -= 8; reasons.append("Liquidity sweep сверху: возможный шорт-откат")
    if res_line and price > res_line[2]: score += 7; reasons.append("Пробой наклонного сопротивления")
    if sup_line and price < sup_line[2]: score -= 7; reasons.append("Пробой наклонной поддержки")
    score += wave["bias"] * 12; reasons.append(f"Elliott Wave: {wave['label']} · confidence {wave['confidence']}% · target {wave['target']:,.2f}")
    cvd_delta = float(df.cvd.iloc[-1] - df.cvd.iloc[-30]); score += 5 if cvd_delta > 0 else -5; reasons.append("CVD/orderflow: " + ("покупатель сильнее" if cvd_delta > 0 else "продавец сильнее"))
    if ctx.get("depth_imbalance") is not None:
        imb = ctx["depth_imbalance"]; score += 5*np.tanh(imb*3); reasons.append(f"Orderbook imbalance: {imb:+.2%}")
    if ctx.get("funding") is not None:
        fr = ctx["funding"]; score -= np.sign(fr)*min(abs(fr)*50000, 4); reasons.append(f"Funding Binance Futures: {fr*100:.4f}%")
    if price > vp["poc"]: score += 4 * reg["weights"]["trend"]; reasons.append(f"VPVR: цена выше POC {vp['poc']:,.2f}")
    else: score -= 4 * reg["weights"]["trend"]; reasons.append(f"VPVR: цена ниже POC {vp['poc']:,.2f}")
    if vp["val"] <= price <= vp["vah"]: reasons.append(f"Value Area: внутри {vp['val']:,.2f}–{vp['vah']:,.2f}")
    else: score += (3 if price > vp["vah"] else -3) * reg["weights"]["trend"]; reasons.append("Value Area breakout/acceptance")
    up_dist = abs(heat["magnet_up"] - price); down_dist = abs(price - heat["magnet_down"])
    if up_dist < down_dist: score += 3.5 * reg["weights"]["liquidity"]; reasons.append(f"Liquidity heatmap: ближайший магнит сверху {heat['magnet_up']:,.2f}")
    else: score -= 3.5 * reg["weights"]["liquidity"]; reasons.append(f"Liquidity heatmap: ближайший магнит снизу {heat['magnet_down']:,.2f}")
    score += 9 * oflow["delta_strength"] * reg["weights"]["orderflow"]; reasons.append(f"Orderflow delta: {oflow['delta_strength']:+.2%}, dominance: {oflow['dominance']}")
    if oflow["bullish_div"]: score += 7; reasons.append("CVD divergence: bullish accumulation")
    if oflow["bearish_div"]: score -= 7; reasons.append("CVD divergence: bearish distribution")
    if oflow["absorption"]: reasons.append("Orderflow: absorption detected")
    reasons.append(f"Regime AI: {reg['regime']} · vol rank {reg['vol_rank']:.0%} · adaptive weights enabled")

    side = "LONG" if score >= 0 else "SHORT"
    if side == "LONG":
        stop = min(price - 1.35*atr_v, ms["liq_low"] - .15*atr_v); take1, take2, take3 = price+1.15*atr_v, price+2.05*atr_v, price+3.25*atr_v; rr=(take2-price)/max(price-stop,1e-9)
    else:
        stop = max(price + 1.35*atr_v, ms["liq_high"] + .15*atr_v); take1, take2, take3 = price-1.15*atr_v, price-2.05*atr_v, price-3.25*atr_v; rr=(price-take2)/max(stop-price,1e-9)

    winrate, trades = backtest_quality(df, side); wf = walk_forward_validation(df, side); mc = monte_carlo_risk(df, side, price, stop, take2)
    score += (winrate - .5) * 18 + (wf["winrate"] - .5) * 16 + (mc["survival"] - 50) * 0.08
    confidence = int(max(7, min(94, 48 + abs(score) * .68 + (winrate-.5)*12 + (wf["winrate"]-.5)*10 + (mc["survival"]-50)*0.10)))
    long_probability = int(round(100 / (1 + math.exp(-score / 18)))); short_probability = 100 - long_probability
    continuation_probability = int(max(20, min(94, confidence * .72 + mc["survival"] * .20 + wf["stability"] * .08)))
    risk_score = int(max(1, min(99, 100 - mc["survival"] + reg["vol_rank"]*22 + (12 if rr < 1.2 else 0))))
    quality = setup_quality(confidence, rr, wf["winrate"], mc["survival"])
    expected_move_pct = abs((take2 - price) / price * 100)
    decision = "Проходимость: высокая" if confidence >= 75 else "Проходимость: средняя" if confidence >= 58 else "Проходимость: низкая / лучше ждать"
    nearest_fib = min(fibs.items(), key=lambda kv: abs(price-kv[1])); reasons.append(f"Ближайший Fibonacci {nearest_fib[0]}: {nearest_fib[1]:,.2f}")
    reasons.append(f"Backtester: winrate {winrate*100:.1f}% на {trades} тест-сделках")
    reasons.append(f"Walk-forward: winrate {wf['winrate']*100:.1f}% · stability {wf['stability']}%")
    reasons.append(f"Monte Carlo: survival {mc['survival']}% · risk of stop {mc['risk_of_stop']}%")
    source_note = ""
    if str(ctx.get("candle_source", "")).startswith("OFFLINE_SYNTHETIC"):
        source_note = "\n⚠️ MARKET DATA OFFLINE: все внешние источники заблокированы/недоступны. Это тестовый режим, НЕ реальные рыночные данные.\n"

    text = (
        f"🏦 FULL AI QUANT SYSTEM v9\n📊 {symbol} · candles: {ctx.get('candle_source', 'unknown')} · TF {PRIMARY_TF}\nЦена: {price:,.2f}\nРежим рынка: {reg['regime']}\n\n"
        f"{source_note}Решение: {side}\nLONG probability: {long_probability}%\nSHORT probability: {short_probability}%\nConfidence: {confidence}%\nContinuation probability: {continuation_probability}%\nSetup Quality: {quality}\nRisk Score: {risk_score}/100\n{decision}\nAI/ML score: {score:+.1f}\n\n"
        f"Вход: {price:,.2f}\nStop: {stop:,.2f}\nTake 1: {take1:,.2f}\nTake 2: {take2:,.2f}\nTake 3: {take3:,.2f}\nRR к TP2: {rr:.2f}\nExpected move: {expected_move_pct:.2f}%\n\n"
        f"Elliott: {wave['label']} · {wave['confidence']}% · invalidation {wave['invalidation']:,.2f}\n"
        f"VPVR POC/VAH/VAL: {vp['poc']:,.2f} / {vp['vah']:,.2f} / {vp['val']:,.2f}\nHeatmap magnets: up {heat['magnet_up']:,.2f}, down {heat['magnet_down']:,.2f}\nOrderflow: {oflow['dominance']} · delta {oflow['delta_strength']:+.2%}\nOpen interest: {ctx.get('open_interest') or 'n/a'}\n\n"
        "Факторы:\n- " + "\n- ".join(reasons[:16]) + "\n\n⚠️ Не финансовый совет. Вероятность — модельный confidence, не гарантия прибыли."
    )
    signal = Signal(symbol, side, confidence, long_probability, short_probability, continuation_probability, price, stop, take1, take2, take3, rr, text, reg["regime"], score, quality, risk_score, expected_move_pct, wave["label"], wave["confidence"], mc["survival"], wf["winrate"])
    meta = {"mtf": mtf, "ctx": ctx, "vp": {k:v for k,v in vp.items() if k != "bins"}, "heat": heat, "oflow": oflow, "wave": wave, "wf": wf, "mc": mc, "reasons": reasons}
    save_signal(signal, meta)
    return signal, {"df": df, "frames": frames, "highs": highs, "lows": lows, "res_line": res_line, "sup_line": sup_line, "fibs": fibs, "ms": ms, "ctx": ctx, "vp": vp, "heat": heat, "oflow": oflow, "reg": reg, "wave": wave, "meta": meta}


def make_chart(symbol, data, signal):
    df = data["df"].tail(180).reset_index(drop=True); x = np.arange(len(df)); fig, ax = plt.subplots(figsize=(14, 8))
    for i, row in df.iterrows():
        ax.vlines(i, row.low, row.high, linewidth=.8)
        ax.add_patch(plt.Rectangle((i-.32, min(row.open,row.close)), .64, max(abs(row.close-row.open), .1), fill=False, linewidth=1.0))
    for p in [20, 50, 200]: ax.plot(x, df[f"ema{p}"], label=f"EMA{p}", linewidth=1.1)
    for name, level in data["fibs"].items():
        if np.isfinite(level): ax.axhline(level, linestyle="--", linewidth=.7, alpha=.45)
    for label, level in [("POC", data["vp"]["poc"]), ("VAH", data["vp"]["vah"]), ("VAL", data["vp"]["val"]), ("Wave target", data["wave"]["target"]), ("Wave invalid", data["wave"]["invalidation"])] :
        if np.isfinite(level): ax.axhline(level, linestyle="-", linewidth=.75, alpha=.45); ax.text(1, level, f" {label}", va="center", fontsize=8)
    for z in data["heat"].get("above", [])[:3] + data["heat"].get("below", [])[:3]: ax.axhline(z["level"], linestyle=":", linewidth=.7, alpha=.35)
    offset = len(data["df"])-len(df)
    for label, line in [("resistance", data["res_line"]), ("support", data["sup_line"] )]:
        if line:
            a,b,_ = line; ax.plot(x, a*(x+offset)+b, linestyle="-.", linewidth=1.1, label=label)
    for idx, p, t in data["wave"].get("points", [])[-6:]:
        j = idx - offset
        if 0 <= j < len(df): ax.scatter(j, p, s=35); ax.text(j, p, t, fontsize=9)
    ax.axhline(data["ms"]["liq_high"], linestyle=":", linewidth=1.1, label="liquidity high"); ax.axhline(data["ms"]["liq_low"], linestyle=":", linewidth=1.1, label="liquidity low")
    ax.axhline(signal.stop, linestyle="--", linewidth=1.2, label="STOP"); ax.axhline(signal.take1, linestyle=":", linewidth=1.0, label="TP1"); ax.axhline(signal.take2, linestyle=":", linewidth=1.0, label="TP2"); ax.axhline(signal.take3, linestyle=":", linewidth=1.0, label="TP3")
    ax.annotate(f"{signal.side} {signal.confidence}% | {signal.setup_quality}", xy=(len(df)-1, signal.entry), xytext=(len(df)-42, signal.entry), arrowprops={"arrowstyle":"->","lw":1.8}, fontsize=13)
    step=max(1,len(df)//8); ax.set_xticks(x[::step]); ax.set_xticklabels([df.time.iloc[i].strftime("%m-%d %H:%M") for i in x[::step]], rotation=30, ha="right")
    ax.set_title(f"{symbol} Full AI Quant v5: Elliott, Multi-TF, SMC, VPVR, Heatmap, CVD, MC, Walk-forward")
    ax.set_ylabel("USDT"); ax.grid(True, alpha=.25); ax.legend(loc="best", fontsize=8); fig.tight_layout(); buf=io.BytesIO(); fig.savefig(buf, format="png", dpi=150); plt.close(fig); buf.seek(0); return buf


def menu():
    return InlineKeyboardMarkup([[InlineKeyboardButton("BTC", callback_data="btc"), InlineKeyboardButton("ETH", callback_data="eth")], [InlineKeyboardButton("SOL", callback_data="sol"), InlineKeyboardButton("XRP", callback_data="xrp")], [InlineKeyboardButton("STATS", callback_data="stats"), InlineKeyboardButton("STATUS", callback_data="status")]])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("FULL AI QUANT v9 готов. Одна кнопка монеты = полный анализ: LONG/SHORT %, confidence, Elliott, CVD, VPVR, heatmap, regime AI, Monte Carlo, walk-forward, journal.", reply_markup=menu())

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t0=time.perf_counter(); await update.message.reply_text("pong"); await update.message.reply_text(f"Время отклика: {(time.perf_counter()-t0)*1000:.0f} ms")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uptime=int(time.time()-START_TIME); mem="n/a"
    try:
        import psutil; mem=f"{psutil.Process(os.getpid()).memory_info().rss/1024/1024:.1f} MB"
    except Exception: pass
    await update.message.reply_text(f"Работает: {uptime//3600}h {(uptime%3600)//60}m {uptime%60}s\nПамять: {mem}\nБиржа: Binance Futures\nTF: {','.join(TIMEFRAMES)}\nDB: {DB_PATH}\nAuto signals: {AUTO_SIGNALS_ENABLED}")

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    st = db_stats(8); rows = st["latest"]
    text = f"📈 Journal stats\nВсего сигналов: {st['total']}\n\nПоследние:\n"
    for r in rows:
        text += f"{r[0][:16]} · {r[1]} {r[2]} · conf {r[3]}% · L/S {r[4]}/{r[5]} · {r[7]} · RR {r[8]:.2f}\n"
    await update.message.reply_text(text[:3900])

async def run_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE, key: str):
    target = update.callback_query.message if update.callback_query else update.message; symbol = SYMBOLS[key]
    await target.reply_text(f"Считаю FULL AI QUANT v9 {symbol}: multi-TF, Elliott, SMC, CVD, VPVR, heatmap, orderflow, regime AI, Monte Carlo, walk-forward...")
    try:
        signal, data = await asyncio.to_thread(analyze, symbol); chart = await asyncio.to_thread(make_chart, symbol, data, signal)
        await target.reply_photo(photo=chart, caption=signal.text[:1024])
        if len(signal.text) > 1024: await target.reply_text(signal.text[1024:])
    except Exception as e:
        print(f"[ERROR] analysis failed: {e}")
        await target.reply_text("Ошибка анализа: не удалось получить рыночные данные. В v9 включён no-crash fallback: CCXT → REST → Yahoo Finance → synthetic offline candles. Проверь Railway Logs, если видишь OFFLINE.")

async def btc(update: Update, context: ContextTypes.DEFAULT_TYPE): await run_analysis(update, context, "btc")
async def eth(update: Update, context: ContextTypes.DEFAULT_TYPE): await run_analysis(update, context, "eth")
async def sol(update: Update, context: ContextTypes.DEFAULT_TYPE): await run_analysis(update, context, "sol")
async def xrp(update: Update, context: ContextTypes.DEFAULT_TYPE): await run_analysis(update, context, "xrp")

async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query; await q.answer()
    if q.data in SYMBOLS: await run_analysis(update, context, q.data)
    elif q.data == "stats":
        st = db_stats(6); await q.message.reply_text(f"Всего сигналов в journal: {st['total']}\nКоманда /stats покажет последние записи.")
    elif q.data == "status":
        uptime=int(time.time()-START_TIME); await q.message.reply_text(f"Работает: {uptime//3600}h {(uptime%3600)//60}m {uptime%60}s")

async def auto_signal_job(context: ContextTypes.DEFAULT_TYPE):
    if not (AUTO_SIGNALS_ENABLED and AUTO_SIGNAL_CHAT_ID): return
    for key, symbol in SYMBOLS.items():
        try:
            signal, data = await asyncio.to_thread(analyze, symbol)
            if signal.confidence >= AUTO_SIGNAL_MIN_CONFIDENCE and signal.setup_quality in ["A+", "A"]:
                chart = await asyncio.to_thread(make_chart, symbol, data, signal)
                await context.bot.send_photo(chat_id=AUTO_SIGNAL_CHAT_ID, photo=chart, caption=("AUTO SIGNAL\n" + signal.text)[:1024])
                if len(signal.text) > 1000: await context.bot.send_message(chat_id=AUTO_SIGNAL_CHAT_ID, text=signal.text[1000:])
        except Exception as e:
            try: await context.bot.send_message(chat_id=AUTO_SIGNAL_CHAT_ID, text=f"Auto signal error {symbol}: {e}")
            except Exception: pass

class DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args): return
    def do_GET(self):
        path = urlparse(self.path).path
        st = db_stats(25)
        if path == "/api/stats":
            body = json.dumps(st, default=str, ensure_ascii=False).encode("utf-8")
            self.send_response(200); self.send_header("Content-Type", "application/json; charset=utf-8"); self.end_headers(); self.wfile.write(body); return
        rows = "".join([f"<tr><td>{r[0][:19]}</td><td>{r[1]}</td><td>{r[2]}</td><td>{r[3]}%</td><td>{r[4]}/{r[5]}</td><td>{r[6]}</td><td>{r[7]}</td><td>{r[8]:.2f}</td></tr>" for r in st["latest"]])
        html = f"""<html><head><meta charset='utf-8'><title>Full AI Quant v5</title><style>body{{font-family:Arial;margin:32px}}table{{border-collapse:collapse;width:100%}}td,th{{border:1px solid #ddd;padding:8px}}th{{background:#f4f4f4}}</style></head><body><h1>Full AI Quant v5 Dashboard</h1><p>Total signals: {st['total']}</p><p><a href='/api/stats'>JSON API</a></p><table><tr><th>Time</th><th>Symbol</th><th>Side</th><th>Conf</th><th>L/S</th><th>Regime</th><th>Quality</th><th>RR</th></tr>{rows}</table></body></html>""".encode("utf-8")
        self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.end_headers(); self.wfile.write(html)

def start_dashboard():
    if not ENABLE_DASHBOARD: return
    def serve():
        try: ThreadingHTTPServer(("0.0.0.0", DASHBOARD_PORT), DashboardHandler).serve_forever()
        except Exception as e: print("Dashboard error:", e)
    threading.Thread(target=serve, daemon=True).start()


def main():
    init_db(); start_dashboard()
    token=os.getenv("TELEGRAM_BOT_TOKEN")
    if not token: raise RuntimeError("Set TELEGRAM_BOT_TOKEN environment variable")
    app=Application.builder().token(token).build()
    for cmd, fn in [("start",start),("btc",btc),("eth",eth),("sol",sol),("xrp",xrp),("ping",ping),("status",status),("stats",stats_cmd)]: app.add_handler(CommandHandler(cmd, fn))
    app.add_handler(CallbackQueryHandler(buttons))
    if AUTO_SIGNALS_ENABLED:
        app.job_queue.run_repeating(auto_signal_job, interval=AUTO_SIGNAL_INTERVAL_MIN*60, first=30)
    app.run_polling(close_loop=False)

if __name__ == "__main__": main()
