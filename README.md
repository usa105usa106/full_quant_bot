# Full AI Quant System v10 Railway Forced

Railway-ready Telegram bot.

Required variable:

```env
TELEGRAM_BOT_TOKEN=your_bot_token
```

Optional:

```env
MARKET_DATA_PROXY_URL=https://your-proxy/candles?symbol={symbol}&interval={interval}&limit={limit}
ALLOW_SYNTHETIC_FALLBACK=true
```

Buttons BTC/ETH/SOL/XRP run full one-click analysis with chart.

Data source is printed in every report:
- `REAL: ...` = real market data
- `DEMO: OFFLINE_SYNTHETIC_NOT_REAL_MARKET_DATA` = only UI/test fallback
