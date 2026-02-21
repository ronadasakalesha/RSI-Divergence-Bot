# RSI-Divergence-Bot

A Python bot that replicates the **RSI with Close & Tail Divergences** Pine Script indicator and sends real-time alerts to Telegram.

## Features
- Fetches live 5-minute BTCUSD candles from **Delta Exchange India**
- Detects **Bearish & Bullish RSI Divergences** (exact Pine Script port)
- Confirms **BUY / SELL signals** on breakout + RSI filter
- Sends instant **Telegram alerts**
- Auto-syncs to candle close boundaries (no drift)

## Setup

```bash
pip install requests pandas pandas_ta python-dotenv
```

Create a `.env` file (see `.env.example`):
```
TELEGRAM_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

## Run
```bash
python rsi_divergence_bot.py
```

## Deploy (PythonAnywhere)
Upload files → `pip3.11 install` dependencies → add as **Always-On Task**.

## Alert Types
| Alert | Condition |
|---|---|
| 🔴 Bearish Divergence | Price higher high, RSI lower high |
| 🟢 Bullish Divergence | Price lower low, RSI higher low  |
| 🔻 SELL Confirmed | Price breaks below divergence low + RSI < 60 |
| ✅ BUY Confirmed  | Price breaks above divergence high + RSI > 40 |
