# Crypto TA Telegram Bot v9 Railway No-Crash

Исправление v9:
- Меню one-click: BTC/ETH/SOL/XRP.
- CCXT + REST fallback.
- Дополнительный fallback через Yahoo Finance.
- Last-resort OFFLINE synthetic candles, чтобы бот не падал, если Railway/регион блокирует все рыночные API.
- В отчёте явно показывает источник свечей: `candles: ...`.
- Если видишь `OFFLINE_SYNTHETIC_NOT_REAL_MARKET_DATA`, значит внешние рыночные данные недоступны из Railway; бот работает в тестовом режиме, не для реальных сигналов.

Railway variable:
```env
TELEGRAM_BOT_TOKEN=your_bot_token
```

Start command берётся из Procfile.
