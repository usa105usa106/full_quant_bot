# FULL AI QUANT v11 RAILWAY PRO

Railway-ready Telegram bot for BTC/ETH/SOL/XRP one-click AI analysis.

## Required Railway variable
`TELEGRAM_BOT_TOKEN=your_botfather_token`

## Optional variables
- `ALLOW_SYNTHETIC_FALLBACK=true` — if all real data APIs are down, bot shows DEMO mode instead of crashing.
- `REDIS_URL=...` — optional Railway Redis cache. If absent, bot uses in-memory TTL cache.
- `CACHE_TTL_SECONDS=55` — candle cache TTL.
- `MARKET_DATA_PROXY_URL=...` — optional custom proxy returning rows.

## v11 changes
- HTF Alignment Engine: 15m/1h/4h/1d weighted direction and conflict score.
- Session AI: Asia/London/New York/Post-NY market behavior context.
- Liquidity Map v2: upper/lower liquidity, sweep probability, fake breakout risk.
- Signal Grades: A+, A, B, C, AVOID, DEMO.
- Dynamic RR Engine: adaptive TP/SL by trend/range/high-volatility regime.
- AI Trade Memory: journal-based adaptive bias from recent bot signals.
- Parallel multi-timeframe loading for faster Railway execution.
- Optional Redis cache + default in-memory cache to reduce API requests.
- Keeps v10 Railway-safe market fallbacks: OKX, Bybit, Binance mirrors, Coinbase, CoinGecko, synthetic demo fallback.

## Deploy
1. Upload to GitHub.
2. Connect repo to Railway.
3. Add `TELEGRAM_BOT_TOKEN`.
4. Deploy.

If report shows `✅ REAL MARKET DATA`, analysis is based on real candles. If it shows `⚠️ DEMO`, do not trade it.
