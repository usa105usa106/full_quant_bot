# Crypto TA Telegram Bot v5 — Full AI Quant System

Railway-ready Telegram bot. Одна кнопка монеты запускает полный AI/quant-анализ и отправляет текст + график.

## Главное
- One-click меню: BTC / ETH / SOL / XRP.
- Итог: LONG probability %, SHORT probability %, Confidence %, Continuation probability %, Setup Quality, Risk Score.
- Автоматические Entry, Stop, TP1/TP2/TP3, RR и Expected Move.
- График с TP/SL, EMA, Fibonacci, liquidity, VPVR, heatmap, Elliott target/invalidation.

## Quant/AI модули
- Binance Futures OHLCV, funding, open interest, orderbook imbalance.
- Multi-timeframe анализ: 15m, 1h, 4h, 1d.
- Elliott Wave engine: impulse/ABC approximation, wave confidence, target, invalidation.
- Smart Money: BOS, CHOCH, liquidity sweep, liquidity high/low.
- CVD/orderflow: delta strength, buyers/sellers dominance, divergence, absorption.
- VPVR / Volume Profile: POC, VAH, VAL, HVN/LVN.
- Synthetic liquidity heatmap: liquidity magnets above/below.
- Regime AI: trend / range / high volatility.
- Adaptive scoring: веса сигналов меняются под режим рынка.
- Walk-forward validation.
- Monte Carlo risk simulation.
- SQLite signal journal.
- Auto-signal scheduler для A/A+ сигналов.
- Mini web dashboard + JSON API.

## Команды
/start — открыть кнопки
/btc — полный анализ BTCUSDT
/eth — полный анализ ETHUSDT
/sol — полный анализ SOLUSDT
/xrp — полный анализ XRPUSDT
/stats — статистика журнала сигналов
/status — статус
/ping — проверка ответа

## Railway переменные
Обязательно:
```
TELEGRAM_BOT_TOKEN=твой_токен_от_BotFather
```

Опционально:
```
TIMEFRAMES=15m,1h,4h,1d
PRIMARY_TF=1h
DEFAULT_LIMIT=500
DATABASE_PATH=signals.db
ENABLE_DASHBOARD=true
AUTO_SIGNALS_ENABLED=false
AUTO_SIGNAL_CHAT_ID=твой_chat_id
AUTO_SIGNAL_MIN_CONFIDENCE=76
AUTO_SIGNAL_INTERVAL_MIN=15
```

## Dashboard
Если `ENABLE_DASHBOARD=true`, Railway даст публичный URL.
- `/` — простая HTML-страница со статистикой.
- `/api/stats` — JSON статистика.

## Важно
Это аналитический бот. Вероятности — модельный confidence, не гарантия прибыли и не финансовый совет.
