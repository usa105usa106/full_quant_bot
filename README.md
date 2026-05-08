# Crypto TA Telegram Bot v12 Railway Pro

Railway-ready Telegram bot for one-click and free-text AI quant analysis.

## What is included
- Buttons: BTC / ETH / SOL / XRP.
- Any coin by text: `ton`, `pol`, `not`, `dogs`, `pepe`, `bnb`, etc.
- Command: `/analyze ton` or `/analyze POLUSDT`.
- Multi-timeframe analysis: 15m / 1h / 4h / 1d.
- Elliott Wave approximation, CVD, VPVR, liquidity map v2, session AI, HTF alignment, dynamic RR.
- Signal output: LONG %, SHORT %, confidence %, continuation %, Entry, SL, TP1/TP2/TP3, RR.
- Chart output in Telegram.
- Journal/statistics with SQLite.
- Optional Redis cache via `REDIS_URL`.
- Railway-safe market data fallbacks.

## Market data sources
The bot tries providers in order and shows the source in every report:
- Custom proxy if `MARKET_DATA_PROXY_URL` is set
- Binance Spot mirrors
- Bybit Spot
- OKX Spot
- BingX Spot
- MEXC Spot
- Coinbase
- CoinGecko
- DEMO synthetic fallback only if all real sources fail

If the report says `✅ REAL MARKET DATA`, the candles are real.
If it says `⚠️ DEMO / НЕ РЫНОЧНЫЕ ДАННЫЕ`, do not trade from that analysis.

## Railway variables
Required:
```env
TELEGRAM_BOT_TOKEN=your_botfather_token
```

Optional:
```env
REDIS_URL=redis://...
ALLOW_SYNTHETIC_FALLBACK=true
CACHE_TTL_SECONDS=55
MARKET_DATA_PROXY_URL=https://your-proxy.example/candles?symbol={symbol}&interval={interval}&limit={limit}
```

## Run locally
```bash
pip install -r requirements.txt
export TELEGRAM_BOT_TOKEN=...
python bot.py
```
