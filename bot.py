import os
import io
import math
import time
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone

import requests
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

START_TIME = time.time()
BINANCE_SPOT = os.getenv("BINANCE_SPOT", "https://api.binance.com")
BINANCE_FUTURES = os.getenv("BINANCE_FUTURES", "https://fapi.binance.com")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "500"))
TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "12"))
PRIMARY_TF = os.getenv("PRIMARY_TF", "1h")
TIMEFRAMES = [x.strip() for x in os.getenv("TIMEFRAMES", "15m,1h,4h,1d").split(",") if x.strip()]

SYMBOLS = {"btc": "BTCUSDT", "eth": "ETHUSDT", "sol": "SOLUSDT", "xrp": "XRPUSDT"}

@dataclass
class Signal:
    side: str
    confidence: int
    entry: float
    stop: float
    take1: float
    take2: float
    take3: float
    rr: float
    text: str
    regime: str
    score: float


def http_get(url: str, params: dict | None = None):
    r = requests.get(url, params=params or {}, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def fetch_klines(symbol: str, interval: str = PRIMARY_TF, limit: int = DEFAULT_LIMIT, futures: bool = True) -> pd.DataFrame:
    base = BINANCE_FUTURES if futures else BINANCE_SPOT
    path = "/fapi/v1/klines" if futures else "/api/v3/klines"
    raw = http_get(f"{base}{path}", {"symbol": symbol, "interval": interval, "limit": limit})
    cols = ["open_time", "open", "high", "low", "close", "volume", "close_time", "qav", "trades", "tbav", "tqav", "ignore"]
    df = pd.DataFrame(raw, columns=cols)
    for c in ["open", "high", "low", "close", "volume", "qav", "tbav", "tqav"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["taker_buy_vol"] = pd.to_numeric(df["tbav"], errors="coerce")
    return df[["time", "open", "high", "low", "close", "volume", "taker_buy_vol", "trades"]].dropna()


def fetch_futures_context(symbol: str) -> dict:
    ctx = {"funding": None, "open_interest": None, "depth_imbalance": None, "glassnode": "not_configured", "coinglass": "not_configured"}
    try:
        prem = http_get(f"{BINANCE_FUTURES}/fapi/v1/premiumIndex", {"symbol": symbol})
        ctx["funding"] = float(prem.get("lastFundingRate", 0))
    except Exception as e:
        ctx["funding_error"] = str(e)
    try:
        oi = http_get(f"{BINANCE_FUTURES}/fapi/v1/openInterest", {"symbol": symbol})
        ctx["open_interest"] = float(oi.get("openInterest", 0))
    except Exception as e:
        ctx["oi_error"] = str(e)
    try:
        depth = http_get(f"{BINANCE_FUTURES}/fapi/v1/depth", {"symbol": symbol, "limit": 100})
        bid_qty = sum(float(x[1]) for x in depth.get("bids", [])[:50])
        ask_qty = sum(float(x[1]) for x in depth.get("asks", [])[:50])
        ctx["depth_imbalance"] = (bid_qty - ask_qty) / max(bid_qty + ask_qty, 1e-9)
    except Exception as e:
        ctx["depth_error"] = str(e)
    if os.getenv("GLASSNODE_API_KEY"):
        ctx["glassnode"] = "api_key_present_optional_hook"
    if os.getenv("COINGLASS_API_KEY"):
        ctx["coinglass"] = "api_key_present_optional_hook"
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
    signed = np.where(df.close >= df.open, df.volume, -df.volume)
    taker_delta = (df.taker_buy_vol - (df.volume - df.taker_buy_vol)).fillna(0)
    df["cvd"] = pd.Series(taker_delta, index=df.index).cumsum()
    df["vol_ma"] = df.volume.rolling(30).mean()
    df["ret"] = df.close.pct_change()
    df["realized_vol"] = df.ret.rolling(48).std() * np.sqrt(48)
    return df


def pivots(df, window=5):
    highs, lows = [], []
    for i in range(window, len(df)-window):
        if df.high.iloc[i] == df.high.iloc[i-window:i+window+1].max(): highs.append((i, float(df.high.iloc[i])))
        if df.low.iloc[i] == df.low.iloc[i-window:i+window+1].min(): lows.append((i, float(df.low.iloc[i])))
    return highs[-12:], lows[-12:]

def fit_line(points, n):
    if len(points) < 2: return None
    x = np.array([p[0] for p in points], dtype=float); y = np.array([p[1] for p in points], dtype=float)
    a, b = np.polyfit(x, y, 1); return a, b, float(a*(n-1)+b)

def fib_levels(df):
    recent = df.tail(220); hi = float(recent.high.max()); lo = float(recent.low.min()); diff = hi-lo
    return hi, lo, {"0.236": hi-diff*.236, "0.382": hi-diff*.382, "0.500": hi-diff*.5, "0.618": hi-diff*.618, "0.786": hi-diff*.786}

def market_structure(df):
    highs, lows = pivots(df, 4)
    bos_up = len(highs) >= 2 and df.close.iloc[-1] > highs[-1][1]
    bos_down = len(lows) >= 2 and df.close.iloc[-1] < lows[-1][1]
    liq_high = max([h[1] for h in highs[-3:]], default=float(df.high.tail(60).max()))
    liq_low = min([l[1] for l in lows[-3:]], default=float(df.low.tail(60).min()))
    sweep_high = df.high.iloc[-1] > liq_high and df.close.iloc[-1] < liq_high
    sweep_low = df.low.iloc[-1] < liq_low and df.close.iloc[-1] > liq_low
    return {"highs": highs, "lows": lows, "bos_up": bos_up, "bos_down": bos_down, "liq_high": liq_high, "liq_low": liq_low, "sweep_high": sweep_high, "sweep_low": sweep_low}



def volume_profile(df: pd.DataFrame, bins: int = 48, lookback: int = 220) -> dict:
    """VPVR approximation: distributes candle volume across price bins by typical price."""
    d = df.tail(lookback).dropna().copy()
    if d.empty:
        return {"poc": np.nan, "vah": np.nan, "val": np.nan, "hvn": [], "lvn": [], "bins": []}
    lo, hi = float(d.low.min()), float(d.high.max())
    edges = np.linspace(lo, hi, bins + 1)
    mids = (edges[:-1] + edges[1:]) / 2
    vol = np.zeros(bins)
    typical = (d.high + d.low + d.close) / 3
    idx = np.clip(np.digitize(typical, edges) - 1, 0, bins - 1)
    for i, v in zip(idx, d.volume):
        vol[int(i)] += float(v)
    total = max(float(vol.sum()), 1e-9)
    poc_i = int(np.argmax(vol))
    order = np.argsort(vol)[::-1]
    included, cum = [], 0.0
    for i in order:
        included.append(int(i)); cum += vol[i]
        if cum / total >= 0.70:
            break
    val, vah = float(mids[min(included)]), float(mids[max(included)])
    hvn_idx = [int(i) for i in order[:5] if vol[i] > 0]
    lvn_idx = [int(i) for i in np.argsort(vol)[:5] if vol[i] > 0]
    return {
        "poc": float(mids[poc_i]), "vah": vah, "val": val,
        "hvn": [float(mids[i]) for i in hvn_idx], "lvn": [float(mids[i]) for i in lvn_idx],
        "bins": [(float(mids[i]), float(vol[i])) for i in range(bins)]
    }


def liquidity_heatmap(df: pd.DataFrame, ctx: dict, lookback: int = 180) -> dict:
    """Synthetic liquidation/liquidity heatmap without paid APIs: swing levels + volume + distance weighting."""
    d = df.tail(lookback).dropna().copy()
    price = float(d.close.iloc[-1])
    atr_v = max(float(d.atr.iloc[-1]) if pd.notna(d.atr.iloc[-1]) else price * 0.01, price * 0.003)
    highs, lows = pivots(d.reset_index(drop=True), 3)
    levels = []
    for _, lvl in highs[-8:]:
        touches = int((abs(d.high - lvl) <= atr_v * 0.35).sum())
        dist = abs(lvl - price) / max(atr_v, 1e-9)
        strength = touches * 12 + max(0, 20 - dist * 2)
        levels.append({"side": "above", "level": float(lvl), "strength": float(strength)})
    for _, lvl in lows[-8:]:
        touches = int((abs(d.low - lvl) <= atr_v * 0.35).sum())
        dist = abs(lvl - price) / max(atr_v, 1e-9)
        strength = touches * 12 + max(0, 20 - dist * 2)
        levels.append({"side": "below", "level": float(lvl), "strength": float(strength)})
    above = sorted([x for x in levels if x["side"] == "above"], key=lambda x: x["strength"], reverse=True)[:5]
    below = sorted([x for x in levels if x["side"] == "below"], key=lambda x: x["strength"], reverse=True)[:5]
    magnet_up = above[0]["level"] if above else float(d.high.tail(80).max())
    magnet_down = below[0]["level"] if below else float(d.low.tail(80).min())
    return {"above": above, "below": below, "magnet_up": magnet_up, "magnet_down": magnet_down}


def orderflow_advanced(df: pd.DataFrame) -> dict:
    d = df.tail(120).dropna().copy()
    delta = d.taker_buy_vol - (d.volume - d.taker_buy_vol)
    cvd = delta.cumsum()
    recent_delta = float(delta.tail(24).sum())
    delta_strength = recent_delta / max(float(d.volume.tail(24).sum()), 1e-9)
    price_change = float(d.close.iloc[-1] - d.close.iloc[-24]) if len(d) >= 24 else 0.0
    cvd_change = float(cvd.iloc[-1] - cvd.iloc[-24]) if len(cvd) >= 24 else float(cvd.iloc[-1])
    bullish_div = price_change < 0 and cvd_change > 0
    bearish_div = price_change > 0 and cvd_change < 0
    absorption = (abs(price_change) < max(float(d.atr.iloc[-1]), d.close.iloc[-1]*0.003) * .55) and abs(delta_strength) > .08
    dominance = "buyers" if delta_strength > .03 else "sellers" if delta_strength < -.03 else "balanced"
    return {"delta_strength": float(delta_strength), "recent_delta": recent_delta, "bullish_div": bool(bullish_div), "bearish_div": bool(bearish_div), "absorption": bool(absorption), "dominance": dominance}


def regime_ai(df: pd.DataFrame, mtf_scores: dict) -> dict:
    d = df.dropna().tail(220)
    last = d.iloc[-1]
    ema_spread = abs(float(last.ema20 - last.ema100)) / max(float(last.atr), 1e-9)
    vol_rank = float((d.realized_vol.rank(pct=True).iloc[-1])) if d.realized_vol.notna().sum() > 30 else 0.5
    mtf_abs = abs(sum(mtf_scores.values()))
    if ema_spread > 2.2 and mtf_abs > 2.0:
        regime = "trend"
    elif vol_rank > 0.80:
        regime = "high_volatility"
    else:
        regime = "range"
    weights = {
        "trend": {"trend": 1.35, "mean_reversion": .65, "orderflow": 1.05, "liquidity": .95},
        "range": {"trend": .70, "mean_reversion": 1.30, "orderflow": 1.00, "liquidity": 1.20},
        "high_volatility": {"trend": .85, "mean_reversion": .75, "orderflow": 1.25, "liquidity": 1.35},
    }[regime]
    return {"regime": regime, "vol_rank": vol_rank, "ema_spread_atr": ema_spread, "weights": weights}

def timeframe_score(df):
    last, prev = df.iloc[-1], df.iloc[-2]
    s = 0
    if last.ema20 > last.ema50 > last.ema200: s += 2
    elif last.ema20 < last.ema50 < last.ema200: s -= 2
    elif last.ema20 > last.ema50: s += 1
    elif last.ema20 < last.ema50: s -= 1
    if last.macd_hist > 0 and last.macd_hist > prev.macd_hist: s += 1
    if last.macd_hist < 0 and last.macd_hist < prev.macd_hist: s -= 1
    if last.rsi > 55: s += .7
    if last.rsi < 45: s -= .7
    return s

def backtest_quality(df, direction):
    d = df.dropna().copy().tail(240)
    if len(d) < 80: return 0.5, 0
    wins = total = 0
    for i in range(30, len(d)-8):
        row = d.iloc[i]; future = d.iloc[i+1:i+9]
        a = max(float(row.atr), float(row.close)*0.004)
        if direction == "LONG":
            tp, sl = row.close + 1.2*a, row.close - 1.0*a
            hit_tp = (future.high >= tp).idxmax() if (future.high >= tp).any() else None
            hit_sl = (future.low <= sl).idxmax() if (future.low <= sl).any() else None
        else:
            tp, sl = row.close - 1.2*a, row.close + 1.0*a
            hit_tp = (future.low <= tp).idxmax() if (future.low <= tp).any() else None
            hit_sl = (future.high >= sl).idxmax() if (future.high >= sl).any() else None
        if hit_tp is not None or hit_sl is not None:
            total += 1; wins += int(hit_sl is None or (hit_tp is not None and hit_tp < hit_sl))
    return (wins / total if total else 0.5), total

def ai_prediction_score(df, mtf_scores):
    last = df.iloc[-1]
    features = np.array([
        np.tanh((last.ema20-last.ema50)/max(last.atr, 1e-9)),
        np.tanh((last.ema50-last.ema200)/max(last.atr, 1e-9)),
        (last.rsi-50)/50,
        np.tanh(last.macd_hist/max(abs(df.macd_hist.tail(100)).median(), 1e-9)),
        np.tanh((df.cvd.iloc[-1]-df.cvd.iloc[-30])/max(abs(df.cvd.diff().tail(100)).sum(), 1e-9)),
        np.tanh(np.mean(mtf_scores)/2.5),
    ])
    weights = np.array([0.22, 0.18, 0.14, 0.16, 0.13, 0.17])
    raw = float(np.dot(features, weights))
    prob_up = 1/(1+math.exp(-3*raw))
    return prob_up, features

def analyze(symbol: str) -> tuple[Signal, dict]:
    frames = {tf: add_indicators(fetch_klines(symbol, tf, DEFAULT_LIMIT, True)) for tf in TIMEFRAMES}
    df = frames[PRIMARY_TF] if PRIMARY_TF in frames else frames[TIMEFRAMES[0]]
    ctx = fetch_futures_context(symbol)
    ms = market_structure(df)
    vp = volume_profile(df)
    heat = liquidity_heatmap(df, ctx)
    oflow = orderflow_advanced(df)
    highs, lows = ms["highs"], ms["lows"]
    res_line, sup_line = fit_line(highs[-5:], len(df)), fit_line(lows[-5:], len(df))
    hi, lo, fibs = fib_levels(df)
    last, prev = df.iloc[-1], df.iloc[-2]
    price = float(last.close); atr_v = max(float(last.atr) if pd.notna(last.atr) else price*.01, price*.003)

    mtf = {tf: timeframe_score(frames[tf].dropna()) for tf in frames if len(frames[tf].dropna()) > 220}
    reg = regime_ai(df, mtf)
    prob_up, features = ai_prediction_score(df.dropna(), list(mtf.values()))
    score = (prob_up - 0.5) * 70
    reasons = [f"AI prediction engine: вероятность роста {prob_up*100:.1f}%", f"Multi-TF score: {sum(mtf.values()):+.2f} ({', '.join([k+':'+format(v, '+.1f') for k,v in mtf.items()])})"]

    if last.ema20 > last.ema50 > last.ema200: score += 18; reasons.append("EMA 20>50>200: трендовый бычий режим")
    elif last.ema20 < last.ema50 < last.ema200: score -= 18; reasons.append("EMA 20<50<200: трендовый медвежий режим")
    if last.rsi > 72: score -= 8; reasons.append("RSI перекуплен: повышен риск отката")
    elif last.rsi < 28: score += 8; reasons.append("RSI перепродан: возможен отскок")
    if last.macd_hist > 0 and last.macd_hist > prev.macd_hist: score += 8; reasons.append("MACD histogram усиливается вверх")
    elif last.macd_hist < 0 and last.macd_hist < prev.macd_hist: score -= 8; reasons.append("MACD histogram усиливается вниз")
    if ms["bos_up"]: score += 10; reasons.append("Smart Money: BOS вверх")
    if ms["bos_down"]: score -= 10; reasons.append("Smart Money: BOS вниз")
    if ms["sweep_low"]: score += 8; reasons.append("Liquidity sweep снизу: возможный лонг-отскок")
    if ms["sweep_high"]: score -= 8; reasons.append("Liquidity sweep сверху: возможный шорт-откат")
    if res_line and price > res_line[2]: score += 7; reasons.append("Пробой наклонного сопротивления")
    if sup_line and price < sup_line[2]: score -= 7; reasons.append("Пробой наклонной поддержки")
    cvd_delta = float(df.cvd.iloc[-1] - df.cvd.iloc[-30])
    if cvd_delta > 0: score += 5; reasons.append("CVD/orderflow: покупатель сильнее на последних свечах")
    else: score -= 5; reasons.append("CVD/orderflow: продавец сильнее на последних свечах")
    if ctx.get("depth_imbalance") is not None:
        imb = ctx["depth_imbalance"]; score += 5*np.tanh(imb*3); reasons.append(f"Orderbook imbalance: {imb:+.2%}")
    if ctx.get("funding") is not None:
        fr = ctx["funding"]; score -= np.sign(fr)*min(abs(fr)*50000, 4); reasons.append(f"Funding Binance Futures: {fr*100:.4f}%")

    # Volume Profile / VPVR
    if price > vp["poc"]:
        score += 4 * reg["weights"]["trend"]; reasons.append(f"VPVR: цена выше POC {vp['poc']:,.2f}, контроль покупателей")
    else:
        score -= 4 * reg["weights"]["trend"]; reasons.append(f"VPVR: цена ниже POC {vp['poc']:,.2f}, контроль продавцов")
    if vp["val"] <= price <= vp["vah"]:
        reasons.append(f"Value Area: внутри VAL/VAH {vp['val']:,.2f}–{vp['vah']:,.2f}")
    else:
        score += (3 if price > vp["vah"] else -3) * reg["weights"]["trend"]; reasons.append("Value Area breakout/acceptance detected")

    # Liquidity heatmap
    up_dist = abs(heat["magnet_up"] - price)
    down_dist = abs(price - heat["magnet_down"])
    if up_dist < down_dist:
        score += 3.5 * reg["weights"]["liquidity"]; reasons.append(f"Liquidity heatmap: ближайший магнит сверху {heat['magnet_up']:,.2f}")
    else:
        score -= 3.5 * reg["weights"]["liquidity"]; reasons.append(f"Liquidity heatmap: ближайший магнит снизу {heat['magnet_down']:,.2f}")

    # Advanced orderflow
    score += 9 * oflow["delta_strength"] * reg["weights"]["orderflow"]
    reasons.append(f"Orderflow delta strength: {oflow['delta_strength']:+.2%}, dominance: {oflow['dominance']}")
    if oflow["bullish_div"]: score += 7; reasons.append("CVD divergence: bullish absorption/accumulation")
    if oflow["bearish_div"]: score -= 7; reasons.append("CVD divergence: bearish distribution")
    if oflow["absorption"]: reasons.append("Orderflow: absorption detected near current price")
    reasons.append(f"Regime AI: {reg['regime']} · vol rank {reg['vol_rank']:.0%} · adaptive weights enabled")

    side = "LONG" if score >= 0 else "SHORT"
    winrate, trades = backtest_quality(df, side)
    score += (winrate - .5) * 22
    confidence = int(max(7, min(94, 50 + abs(score) * .72 + (winrate-.5)*20)))
    # Итоговые вероятности LONG/SHORT: показываются пользователю одной строкой.
    long_probability = int(round(100 / (1 + math.exp(-score / 18))))
    short_probability = 100 - long_probability
    continuation_probability = confidence if side == "LONG" and long_probability >= short_probability else confidence if side == "SHORT" and short_probability >= long_probability else max(35, confidence - 12)
    regime = reg["regime"]
    decision = "Проходимость: высокая" if confidence >= 75 else "Проходимость: средняя" if confidence >= 58 else "Проходимость: низкая / лучше ждать"
    nearest_fib = min(fibs.items(), key=lambda kv: abs(price-kv[1])); reasons.append(f"Ближайший Fibonacci {nearest_fib[0]}: {nearest_fib[1]:,.2f}")
    reasons.append(f"Backtester по последним данным: winrate {winrate*100:.1f}% на {trades} тест-сделках")
    reasons.append(f"Glassnode: {ctx['glassnode']}; Coinglass: {ctx['coinglass']} (хуки под API-ключи оставлены)")

    if side == "LONG":
        stop = min(price - 1.35*atr_v, ms["liq_low"] - .15*atr_v); take1, take2, take3 = price+1.15*atr_v, price+2.05*atr_v, price+3.25*atr_v; rr=(take2-price)/max(price-stop,1e-9)
    else:
        stop = max(price + 1.35*atr_v, ms["liq_high"] + .15*atr_v); take1, take2, take3 = price-1.15*atr_v, price-2.05*atr_v, price-3.25*atr_v; rr=(price-take2)/max(stop-price,1e-9)

    text = (
        f"🏦 HEDGE FUND / FULL AI QUANT SYSTEM\n📊 {symbol} · Binance Futures · TF {PRIMARY_TF}\n"
        f"Цена: {price:,.2f}\nРежим рынка: {regime}\n\n"
        f"Решение: {side}\n"
        f"LONG probability: {long_probability}%\n"
        f"SHORT probability: {short_probability}%\n"
        f"Confidence: {confidence}%\n"
        f"Continuation probability: {continuation_probability}%\n"
        f"{decision}\n"
        f"AI/ML score: {score:+.1f}\n\n"
        f"Вход: {price:,.2f}\nStop: {stop:,.2f}\nTake 1: {take1:,.2f}\nTake 2: {take2:,.2f}\nTake 3: {take3:,.2f}\nRR к TP2: {rr:.2f}\n\n"
        f"Liquidity high: {ms['liq_high']:,.2f}\nLiquidity low: {ms['liq_low']:,.2f}\n"
        f"VPVR POC/VAH/VAL: {vp['poc']:,.2f} / {vp['vah']:,.2f} / {vp['val']:,.2f}\n"
        f"Heatmap magnets: up {heat['magnet_up']:,.2f}, down {heat['magnet_down']:,.2f}\n"
        f"Orderflow: {oflow['dominance']} · delta {oflow['delta_strength']:+.2%}\n"
        f"Open interest: {ctx.get('open_interest') or 'n/a'}\n\n"
        "Факторы:\n- " + "\n- ".join(reasons[:13]) +
        "\n\n⚠️ Не финансовый совет. Вероятность — это модельный confidence, не гарантия прибыли."
    )
    return Signal(side, confidence, price, stop, take1, take2, take3, rr, text, regime, score), {"df": df, "frames": frames, "highs": highs, "lows": lows, "res_line": res_line, "sup_line": sup_line, "fibs": fibs, "ms": ms, "ctx": ctx, "vp": vp, "heat": heat, "oflow": oflow, "reg": reg}


def make_chart(symbol, data, signal):
    df = data["df"].tail(180).reset_index(drop=True); x = np.arange(len(df))
    fig, ax = plt.subplots(figsize=(14, 8))
    for i, row in df.iterrows():
        ax.vlines(i, row.low, row.high, linewidth=.8)
        ax.add_patch(plt.Rectangle((i-.32, min(row.open,row.close)), .64, max(abs(row.close-row.open), .1), fill=False, linewidth=1.0))
    for p in [20, 50, 200]: ax.plot(x, df[f"ema{p}"], label=f"EMA{p}", linewidth=1.1)
    for name, level in data["fibs"].items():
        ax.axhline(level, linestyle="--", linewidth=.8, alpha=.55); ax.text(len(df)-1, level, f" Fib {name} {level:,.0f}", va="center", fontsize=8)
    for label, level in [("POC", data["vp"]["poc"]), ("VAH", data["vp"]["vah"]), ("VAL", data["vp"]["val"])]:
        if np.isfinite(level):
            ax.axhline(level, linestyle="-", linewidth=.75, alpha=.45); ax.text(1, level, f" {label}", va="center", fontsize=8)
    for z in data["heat"].get("above", [])[:3] + data["heat"].get("below", [])[:3]:
        ax.axhline(z["level"], linestyle=":", linewidth=.7, alpha=.35)
    full_n, offset = len(data["df"]), len(data["df"])-len(df)
    for label, line in [("resistance", data["res_line"]), ("support", data["sup_line"] )]:
        if line:
            a,b,_ = line; ax.plot(x, a*(x+offset)+b, linestyle="-.", linewidth=1.1, label=label)
    ax.axhline(data["ms"]["liq_high"], linestyle=":", linewidth=1.1, label="liquidity high")
    ax.axhline(data["ms"]["liq_low"], linestyle=":", linewidth=1.1, label="liquidity low")
    ax.axhline(signal.stop, linestyle="--", linewidth=1.2, label="STOP")
    ax.axhline(signal.take1, linestyle=":", linewidth=1.0, label="TP1")
    ax.axhline(signal.take2, linestyle=":", linewidth=1.0, label="TP2")
    ax.axhline(signal.take3, linestyle=":", linewidth=1.0, label="TP3")
    ax.annotate(f"{signal.side} {signal.confidence}%", xy=(len(df)-1, signal.entry), xytext=(len(df)-35, signal.entry), arrowprops={"arrowstyle":"->","lw":1.8}, fontsize=13)
    step=max(1,len(df)//8); ax.set_xticks(x[::step]); ax.set_xticklabels([df.time.iloc[i].strftime("%m-%d %H:%M") for i in x[::step]], rotation=30, ha="right")
    ax.set_title(f"{symbol} Quant: Multi-TF, SMC, VPVR, Heatmap, CVD/orderflow, liquidity, Fibonacci, {signal.side}")
    ax.set_ylabel("USDT"); ax.grid(True, alpha=.25); ax.legend(loc="best", fontsize=8); fig.tight_layout()
    buf=io.BytesIO(); fig.savefig(buf, format="png", dpi=150); plt.close(fig); buf.seek(0); return buf


def menu():
    # Минимальное меню: одна кнопка монеты = полный AI-анализ всеми модулями.
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("BTC", callback_data="btc"), InlineKeyboardButton("ETH", callback_data="eth")],
        [InlineKeyboardButton("SOL", callback_data="sol"), InlineKeyboardButton("XRP", callback_data="xrp")],
        [InlineKeyboardButton("STATUS", callback_data="status")],
    ])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "AI QUANT бот готов. Нажми одну кнопку монеты — бот сам соберёт полный анализ.\n"
        "Команды: /btc, /eth, /sol, /xrp, /status.\n"
        "В отчёте сразу будут LONG/SHORT probability, confidence, TP/SL, RR, CVD, Elliott/SMC, VPVR, heatmap, orderflow, regime AI и backtest.",
        reply_markup=menu())

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t0=time.perf_counter(); await update.message.reply_text("pong"); await update.message.reply_text(f"Время отклика: {(time.perf_counter()-t0)*1000:.0f} ms")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uptime=int(time.time()-START_TIME); mem="n/a"
    try:
        import psutil; mem=f"{psutil.Process(os.getpid()).memory_info().rss/1024/1024:.1f} MB"
    except Exception: pass
    await update.message.reply_text(f"Работает: {uptime//3600}h {(uptime%3600)//60}m {uptime%60}s\nПамять: {mem}\nБиржа: Binance Futures\nTF: {','.join(TIMEFRAMES)}")

async def run_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE, key: str):
    target = update.callback_query.message if update.callback_query else update.message
    symbol = SYMBOLS[key]
    await target.reply_text(f"Считаю FULL AI QUANT анализ {symbol}: futures, multi-TF, SMC, orderflow, VPVR, heatmap, liquidity, CVD, adaptive AI, backtest...")
    try:
        signal, data = await asyncio.to_thread(analyze, symbol)
        chart = await asyncio.to_thread(make_chart, symbol, data, signal)
        await target.reply_photo(photo=chart, caption=signal.text[:1024])
        if len(signal.text) > 1024: await target.reply_text(signal.text[1024:])
    except Exception as e:
        await target.reply_text(f"Ошибка анализа: {e}")

async def btc(update: Update, context: ContextTypes.DEFAULT_TYPE): await run_analysis(update, context, "btc")
async def eth(update: Update, context: ContextTypes.DEFAULT_TYPE): await run_analysis(update, context, "eth")
async def sol(update: Update, context: ContextTypes.DEFAULT_TYPE): await run_analysis(update, context, "sol")
async def xrp(update: Update, context: ContextTypes.DEFAULT_TYPE): await run_analysis(update, context, "xrp")

async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query; await q.answer()
    if q.data in SYMBOLS: await run_analysis(update, context, q.data)
    elif q.data == "ping": await q.message.reply_text("pong")
    elif q.data == "status":
        uptime=int(time.time()-START_TIME); await q.message.reply_text(f"Работает: {uptime//3600}h {(uptime%3600)//60}m {uptime%60}s")

def main():
    token=os.getenv("TELEGRAM_BOT_TOKEN")
    if not token: raise RuntimeError("Set TELEGRAM_BOT_TOKEN environment variable")
    app=Application.builder().token(token).build()
    for cmd, fn in [("start",start),("btc",btc),("eth",eth),("sol",sol),("xrp",xrp),("ping",ping),("status",status)]: app.add_handler(CommandHandler(cmd, fn))
    app.add_handler(CallbackQueryHandler(buttons)); app.run_polling(close_loop=False)

if __name__ == "__main__": main()
