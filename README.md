# Crypto TA Telegram Bot v7 Railway Safe

Исправление v7: Binance может отдавать HTTP 451 на Railway/cloud IP.
Теперь бот использует цепочку источников данных:

1. Binance Futures
2. Binance Spot
3. Bybit Spot fallback
4. OKX Spot fallback

Переменная Railway остаётся:

```env
TELEGRAM_BOT_TOKEN=your_token
```

Команды/кнопки: BTC, ETH, SOL, XRP, /stats, /status.

Если Binance заблокирован полностью, анализ продолжит работать через Bybit/OKX. Futures-only данные вроде funding/OI будут помечены как недоступные, но AI scoring, Elliott, VPVR, CVD approximation, heatmap, Smart Money, Monte Carlo, walk-forward и график останутся рабочими.
