# Crypto TA Telegram Bot v4 — One-Click AI Quant

Telegram bot для Railway. Пользователь нажимает одну кнопку монеты — бот сам запускает полный анализ и показывает итоговый сигнал.

## Что внутри
- One-click меню: BTC / ETH / SOL / XRP.
- Итоговые проценты: LONG probability, SHORT probability, Confidence, Continuation probability.
- Автоматический вход, Stop, TP1/TP2/TP3, RR.
- Binance Futures данные.
- Multi-TF: 15m, 1h, 4h, 1d.
- Smart Money: BOS, liquidity sweep, liquidity high/low.
- CVD / orderflow: delta strength, dominance, divergence, absorption.
- VPVR / Volume Profile: POC, VAH, VAL, HVN/LVN.
- Liquidity heatmap: зоны ликвидности сверху/снизу и price magnets.
- Regime AI: trend / range / high volatility.
- Adaptive scoring: веса меняются под режим рынка.
- Backtest validation качества сигнала.
- График с EMA, Fibonacci, VPVR, heatmap-зонами, liquidity, TP/SL и стрелкой направления.

## Команды
/start — открыть кнопки
/btc — полный анализ BTCUSDT
/eth — полный анализ ETHUSDT
/sol — полный анализ SOLUSDT
/xrp — полный анализ XRPUSDT
/status — статус

## Railway
Добавь переменную окружения:
```
TELEGRAM_BOT_TOKEN=твой_токен_от_BotFather
```

Опционально:
```
TIMEFRAMES=15m,1h,4h,1d
PRIMARY_TF=1h
DEFAULT_LIMIT=500
```

⚠️ Это аналитический бот, не финансовый совет. Вероятности — модельный confidence, не гарантия прибыли.
