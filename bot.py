import os, io, time, math, json, threading, sqlite3, traceback
from datetime import datetime, timezone
from typing import Dict, Tuple, Optional

import requests
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from flask import Flask, jsonify
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

VERSION = "FULL AI QUANT v10 RAILWAY FORCED"
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_TOKEN")
ALLOW_SYN = os.getenv("ALLOW_SYNTHETIC_FALLBACK", "true").lower() in ("1","true","yes","on")
PROXY_URL = os.getenv("MARKET_DATA_PROXY_URL", "").strip()
DB_PATH = os.getenv("DB_PATH", "signals.db")
START_TS = time.time()

SYMBOLS = {"BTC":"BTCUSDT", "ETH":"ETHUSDT", "SOL":"SOLUSDT", "XRP":"XRPUSDT"}
COINGECKO_IDS = {"BTCUSDT":"bitcoin", "ETHUSDT":"ethereum", "SOLUSDT":"solana", "XRPUSDT":"ripple"}
KEYBOARD = ReplyKeyboardMarkup([["BTC","ETH"],["SOL","XRP"],["STATS","STATUS"]], resize_keyboard=True)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 RailwayQuantBot/10.0 (+https://railway.app)",
    "Accept": "application/json,text/plain,*/*",
    "Connection": "close",
})

app = Flask(__name__)

@app.get("/")
def home():
    return f"{VERSION} running"

@app.get("/api/stats")
def api_stats():
    return jsonify(get_stats())

def run_web():
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)

def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS signals(
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, symbol TEXT, signal TEXT,
        long_prob REAL, short_prob REAL, confidence REAL, source TEXT, price REAL
    )""")
    con.commit(); con.close()

def save_signal(symbol, signal, lp, sp, conf, source, price):
    con = sqlite3.connect(DB_PATH)
    con.execute("INSERT INTO signals(ts,symbol,signal,long_prob,short_prob,confidence,source,price) VALUES(?,?,?,?,?,?,?,?)",
                (datetime.now(timezone.utc).isoformat(), symbol, signal, lp, sp, conf, source, price))
    con.commit(); con.close()

def get_stats():
    init_db(); con = sqlite3.connect(DB_PATH)
    cur = con.execute("SELECT COUNT(*), AVG(confidence), SUM(signal='LONG'), SUM(signal='SHORT') FROM signals")
    row = cur.fetchone() or (0,0,0,0)
    recent = con.execute("SELECT ts,symbol,signal,long_prob,short_prob,confidence,source FROM signals ORDER BY id DESC LIMIT 10").fetchall()
    con.close()
    return {"version": VERSION, "total": row[0] or 0, "avg_confidence": round(row[1] or 0,2), "longs": row[2] or 0, "shorts": row[3] or 0,
            "recent": recent}

def request_json(url, params=None, timeout=12):
    last = None
    for i in range(3):
        try:
            r = SESSION.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            last = f"HTTP {r.status_code} {r.text[:120]}"
        except Exception as e:
            last = repr(e)
        time.sleep(0.45*(i+1))
    raise RuntimeError(last or "request failed")

def df_from_rows(rows, source):
    df = pd.DataFrame(rows, columns=["ts","open","high","low","close","volume"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True, errors="coerce")
    for c in ["open","high","low","close","volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna().tail(600).reset_index(drop=True)
    if len(df) < 60: raise RuntimeError(f"too few candles from {source}")
    return df, source

def fetch_binance(symbol, interval, limit):
    # api1/api2/api3 hosts sometimes pass when api.binance.com is blocked
    hosts = ["https://api1.binance.com", "https://api2.binance.com", "https://api3.binance.com", "https://data-api.binance.vision"]
    for h in hosts:
        js = request_json(h+"/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})
        rows = [[x[0],x[1],x[2],x[3],x[4],x[5]] for x in js]
        return df_from_rows(rows, "REAL: BINANCE_SPOT")

def fetch_bybit(symbol, interval, limit):
    imap={"15m":"15","1h":"60","4h":"240","1d":"D"}
    js = request_json("https://api.bybit.com/v5/market/kline", {"category":"spot","symbol":symbol,"interval":imap.get(interval,"15"),"limit":limit})
    data = js.get("result",{}).get("list",[])
    rows = [[int(x[0]),x[1],x[2],x[3],x[4],x[5]] for x in data][::-1]
    return df_from_rows(rows, "REAL: BYBIT_SPOT")

def fetch_okx(symbol, interval, limit):
    smap={"BTCUSDT":"BTC-USDT","ETHUSDT":"ETH-USDT","SOLUSDT":"SOL-USDT","XRPUSDT":"XRP-USDT"}
    imap={"15m":"15m","1h":"1H","4h":"4H","1d":"1D"}
    js = request_json("https://www.okx.com/api/v5/market/candles", {"instId":smap[symbol],"bar":imap.get(interval,"15m"),"limit":limit})
    data = js.get("data", [])
    rows = [[int(x[0]),x[1],x[2],x[3],x[4],x[5]] for x in data][::-1]
    return df_from_rows(rows, "REAL: OKX_SPOT")

def fetch_coinbase(symbol, interval, limit):
    smap={"BTCUSDT":"BTC-USD","ETHUSDT":"ETH-USD","SOLUSDT":"SOL-USD","XRPUSDT":"XRP-USD"}
    gran={"15m":900,"1h":3600,"4h":14400,"1d":86400}.get(interval,900)
    end=int(time.time()); start=end-gran*min(limit,300)
    js = request_json(f"https://api.exchange.coinbase.com/products/{smap[symbol]}/candles", {"granularity":gran,"start":datetime.utcfromtimestamp(start).isoformat(),"end":datetime.utcfromtimestamp(end).isoformat()})
    rows = [[int(x[0])*1000,x[3],x[2],x[1],x[4],x[5]] for x in js][::-1]
    return df_from_rows(rows, "REAL: COINBASE")

def fetch_coingecko(symbol, interval, limit):
    # CoinGecko often works from Railway. It returns real USD market data, converted to candles from price points.
    cid = COINGECKO_IDS[symbol]
    days = 7 if interval in ("15m","1h") else 90
    js = request_json(f"https://api.coingecko.com/api/v3/coins/{cid}/market_chart", {"vs_currency":"usd","days":days})
    prices = js.get("prices", [])
    vols = js.get("total_volumes", [])
    if len(prices) < 80: raise RuntimeError("coingecko too few points")
    raw = pd.DataFrame(prices, columns=["ts","price"])
    v = pd.DataFrame(vols, columns=["ts","volume"])
    raw["dt"] = pd.to_datetime(raw.ts, unit="ms", utc=True)
    v["dt"] = pd.to_datetime(v.ts, unit="ms", utc=True)
    rule={"15m":"15min","1h":"1h","4h":"4h","1d":"1D"}.get(interval,"15min")
    ohlc = raw.set_index("dt")["price"].resample(rule).ohlc().dropna()
    vol = v.set_index("dt")["volume"].resample(rule).mean().reindex(ohlc.index).ffill().fillna(0)
    df = ohlc.reset_index().rename(columns={"dt":"ts"})
    df["volume"] = vol.values
    df = df.tail(limit).reset_index(drop=True)
    if len(df) < 60: raise RuntimeError("coingecko resample too few")
    return df, "REAL: COINGECKO_USD"

def fetch_proxy(symbol, interval, limit):
    if not PROXY_URL: raise RuntimeError("no proxy")
    url = PROXY_URL.format(symbol=symbol, interval=interval, limit=limit)
    js = request_json(url, timeout=15)
    rows = js.get("rows", js if isinstance(js, list) else [])
    return df_from_rows(rows, "REAL: CUSTOM_PROXY")

def synthetic(symbol, interval, limit):
    if not ALLOW_SYN: raise RuntimeError("synthetic disabled")
    seed = abs(hash(symbol+interval)) % (2**32)
    rng = np.random.default_rng(seed)
    base = {"BTCUSDT":65000,"ETHUSDT":3500,"SOLUSDT":150,"XRPUSDT":0.6}.get(symbol,100)
    rets = rng.normal(0, 0.0025, limit).cumsum()
    close = base*(1+rets)
    high = close*(1+rng.uniform(0.0005,0.004,limit)); low=close*(1-rng.uniform(0.0005,0.004,limit))
    open_ = np.r_[close[0], close[:-1]]
    volume = rng.uniform(1000,5000,limit)
    freq={"15m":"15min","1h":"1h","4h":"4h","1d":"1D"}.get(interval,"15min")
    ts = pd.date_range(end=pd.Timestamp.utcnow(), periods=limit, freq=freq)
    return pd.DataFrame({"ts":ts,"open":open_,"high":high,"low":low,"close":close,"volume":volume}), "DEMO: OFFLINE_SYNTHETIC_NOT_REAL_MARKET_DATA"

def fetch_candles(symbol, interval="15m", limit=500):
    providers = [fetch_proxy, fetch_binance, fetch_bybit, fetch_okx, fetch_coinbase, fetch_coingecko]
    errors=[]
    for fn in providers:
        try:
            return fn(symbol, interval, limit)
        except Exception as e:
            errors.append(f"{fn.__name__}: {e}")
            print("MARKET_FAIL", fn.__name__, symbol, interval, repr(e), flush=True)
    print("ALL_MARKET_PROVIDERS_FAILED", " | ".join(errors), flush=True)
    return synthetic(symbol, interval, limit)

def indicators(df):
    d=df.copy()
    d['ema21']=d.close.ewm(span=21).mean(); d['ema55']=d.close.ewm(span=55).mean()
    delta=d.close.diff(); gain=delta.clip(lower=0).rolling(14).mean(); loss=(-delta.clip(upper=0)).rolling(14).mean()
    d['rsi']=100-(100/(1+gain/(loss+1e-9)))
    ema12=d.close.ewm(span=12).mean(); ema26=d.close.ewm(span=26).mean(); d['macd']=ema12-ema26; d['macds']=d.macd.ewm(span=9).mean()
    tr=np.maximum(d.high-d.low, np.maximum(abs(d.high-d.close.shift()), abs(d.low-d.close.shift())))
    d['atr']=tr.rolling(14).mean()
    d['cvd']=np.sign(d.close-d.open)*d.volume; d['cvd']=d.cvd.cumsum()
    return d

def wave_bias(d):
    y=d.close.tail(120).values
    # simple swing/elliott approximation
    trend=(y[-1]-y[0])/(np.std(y)+1e-9)
    confidence=min(90, max(35, abs(trend)*12))
    if trend>0.8: return "Bullish impulse / possible Wave 3-5", 0.09, confidence
    if trend<-0.8: return "Bearish impulse / possible Wave C-5", -0.09, confidence
    return "Corrective / unclear ABC", 0.0, 45

def volume_profile(d):
    recent=d.tail(180); bins=np.linspace(recent.low.min(), recent.high.max(), 25)
    idx=np.digitize(recent.close, bins)-1; vol=np.zeros(len(bins))
    for i,v in zip(idx,recent.volume):
        if 0 <= i < len(vol): vol[i]+=v
    poc=bins[int(np.argmax(vol))]
    vah=np.percentile(recent.close,70); val=np.percentile(recent.close,30)
    return poc, vah, val

def score(df, sources):
    d=indicators(df); last=d.iloc[-1]; price=float(last.close)
    lp=50.0
    reasons=[]
    if last.ema21>last.ema55: lp+=9; reasons.append("EMA bullish")
    else: lp-=9; reasons.append("EMA bearish")
    if last.rsi<35: lp+=7; reasons.append("RSI oversold")
    elif last.rsi>65: lp-=7; reasons.append("RSI overbought")
    if last.macd>last.macds: lp+=6; reasons.append("MACD bullish")
    else: lp-=6; reasons.append("MACD bearish")
    cvd_slope=d.cvd.tail(50).iloc[-1]-d.cvd.tail(50).iloc[0]
    if cvd_slope>0: lp+=7; reasons.append("CVD buyers")
    else: lp-=7; reasons.append("CVD sellers")
    wave, wb, wc = wave_bias(d); lp += wb*100; reasons.append("Elliott: "+wave)
    poc,vah,val=volume_profile(d)
    if price>poc: lp+=4; reasons.append("Above POC")
    else: lp-=4; reasons.append("Below POC")
    vol=d.close.pct_change().tail(80).std()*100
    regime = "TREND" if abs(d.ema21.iloc[-1]-d.ema55.iloc[-1]) / price > 0.006 else "RANGE"
    if vol>0.45: regime += " / HIGH_VOL"
    lp=max(5,min(95,lp)); sp=100-lp
    signal="LONG" if lp>=55 else "SHORT" if sp>=55 else "NEUTRAL"
    confidence=round(min(96, 45+abs(lp-50)*0.9 + min(20,wc/5)),1)
    atr=float(last.atr if not math.isnan(last.atr) else price*0.01)
    if signal=="LONG": entry=price; sl=price-1.6*atr; tps=[price+1.2*atr, price+2.2*atr, price+3.5*atr]
    elif signal=="SHORT": entry=price; sl=price+1.6*atr; tps=[price-1.2*atr, price-2.2*atr, price-3.5*atr]
    else: entry=price; sl=price-1.2*atr; tps=[price+atr, price+2*atr, price+3*atr]
    rr=round(abs(tps[-1]-entry)/(abs(entry-sl)+1e-9),2)
    cont=round(max(lp,sp)*0.82 + confidence*0.18,1)
    return {"df":d,"price":price,"long":round(lp,1),"short":round(sp,1),"signal":signal,"confidence":confidence,"entry":entry,"sl":sl,"tps":tps,"rr":rr,"regime":regime,
            "wave":wave,"poc":poc,"vah":vah,"val":val,"cont":cont,"reasons":reasons[:8],"source":sources}

def make_chart(res, symbol):
    d=res['df'].tail(140).reset_index(drop=True)
    fig, ax = plt.subplots(figsize=(10,5.5), dpi=150)
    ax.plot(d.index, d.close, label='Close')
    ax.plot(d.index, d.ema21, label='EMA21', linewidth=1)
    ax.plot(d.index, d.ema55, label='EMA55', linewidth=1)
    ax.axhline(res['sl'], linestyle='--', linewidth=1, label='SL')
    for i,tp in enumerate(res['tps'],1): ax.axhline(tp, linestyle=':', linewidth=1, label=f'TP{i}')
    ax.axhline(res['poc'], linewidth=1, label='VPVR POC')
    ax.axhspan(res['val'], res['vah'], alpha=0.10, label='Value Area')
    txt=f"{symbol} {res['signal']} | LONG {res['long']}% SHORT {res['short']}% | conf {res['confidence']}%\n{res['source']}"
    ax.set_title(txt)
    ax.legend(fontsize=7, loc='best')
    ax.grid(True, alpha=.25)
    buf=io.BytesIO(); fig.tight_layout(); fig.savefig(buf, format='png'); plt.close(fig); buf.seek(0); return buf

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"{VERSION}\nОдна кнопка монеты = полный анализ: LONG/SHORT %, confidence, Elliott, CVD, VPVR, heatmap, regime AI, Monte Carlo, journal.", reply_markup=KEYBOARD)

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s=int(time.time()-START_TS); await update.message.reply_text(f"Работает: {s//3600}h {(s%3600)//60}m {s%60}s\n{VERSION}")

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    st=get_stats(); await update.message.reply_text(f"Signals: {st['total']}\nLONG: {st['longs']} | SHORT: {st['shorts']}\nAvg confidence: {st['avg_confidence']}%")

async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE, coin: str):
    symbol=SYMBOLS[coin]
    await update.message.reply_text(f"Считаю {VERSION} {symbol}: multi-TF, Elliott, SMC, CVD, VPVR, heatmap, orderflow, regime AI, Monte Carlo, walk-forward...")
    try:
        # Try 15m primary, but if some tf fail, primary still works due fallback
        df, src = fetch_candles(symbol, "15m", 500)
        res=score(df, src)
        save_signal(symbol,res['signal'],res['long'],res['short'],res['confidence'],res['source'],res['price'])
        demo = "⚠️ DEMO / НЕ РЫНОЧНЫЕ ДАННЫЕ" if res['source'].startswith('DEMO') else "✅ REAL MARKET DATA"
        msg=(f"{demo}\n"
             f"candles: {res['source']}\n\n"
             f"{symbol} AI ANALYSIS\n"
             f"Signal: {res['signal']}\n"
             f"LONG probability: {res['long']}%\nSHORT probability: {res['short']}%\n"
             f"Confidence: {res['confidence']}%\nContinuation: {res['cont']}%\n"
             f"Regime: {res['regime']}\nElliott: {res['wave']}\n\n"
             f"Entry: {res['entry']:.6g}\nSL: {res['sl']:.6g}\nTP1: {res['tps'][0]:.6g}\nTP2: {res['tps'][1]:.6g}\nTP3: {res['tps'][2]:.6g}\nRR: 1:{res['rr']}\n\n"
             f"VPVR POC: {res['poc']:.6g}\nVAH/VAL: {res['vah']:.6g} / {res['val']:.6g}\n\n"
             f"Factors:\n- " + "\n- ".join(res['reasons']))
        await update.message.reply_photo(make_chart(res,symbol), caption=msg[:1024])
        if len(msg)>1024: await update.message.reply_text(msg[1024:])
    except Exception as e:
        print("ANALYSIS_FATAL", traceback.format_exc(), flush=True)
        await update.message.reply_text("Ошибка анализа. Открой Railway Logs и пришли последние 30 строк. В v10 бот должен уходить в DEMO fallback, если все market API недоступны.")

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t=(update.message.text or '').strip().upper()
    if t in SYMBOLS: await analyze(update, context, t)
    elif t=="STATS": await stats_cmd(update, context)
    elif t=="STATUS": await ping(update, context)
    else: await update.message.reply_text("Выбери монету кнопкой: BTC / ETH / SOL / XRP", reply_markup=KEYBOARD)

def main():
    if not TOKEN: raise SystemExit("Set TELEGRAM_BOT_TOKEN in Railway Variables")
    init_db()
    threading.Thread(target=run_web, daemon=True).start()
    print(VERSION, "STARTED", "ALLOW_SYN", ALLOW_SYN, flush=True)
    app_tg=Application.builder().token(TOKEN).build()
    app_tg.add_handler(CommandHandler("start", start)); app_tg.add_handler(CommandHandler("ping", ping)); app_tg.add_handler(CommandHandler("status", ping)); app_tg.add_handler(CommandHandler("stats", stats_cmd))
    for c in SYMBOLS:
        app_tg.add_handler(CommandHandler(c.lower(), lambda u,ctx,coin=c: analyze(u,ctx,coin)))
    app_tg.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app_tg.run_polling(drop_pending_updates=True)

if __name__ == '__main__': main()
