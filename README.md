# RSI-Divergence-Bot

A Python bot that replicates the **RSI with Close & Tail Divergences** Pine Script indicator and sends real-time alerts to Telegram. Supports both Crypto (Delta Exchange) and Indian Equities (Angel One).

## Features
- **Crypto**: Handles BTCUSD from **Delta Exchange India** (24/7).
- **Equities**: Handles Nifty50 from **Angel One SmartAPI** (market hours only).
- Detects **Bearish & Bullish RSI Divergences** (exact Pine Script port).
- Confirms **BUY / SELL signals** on breakout + RSI filter.
- Sends instant **Telegram alerts**.

## Setup

```bash
pip install requests pandas pandas_ta python-dotenv smartapi-python pyotp
```

Create a `.env` file (see `.env.example`):
```bash
TELEGRAM_TOKEN=...
TELEGRAM_CHAT_ID=...
# Optional / Provider specific:
DELTA_API_KEY=...
ANGELONE_CLIENT_ID=...
ANGELONE_PASSWORD=...
ANGELONE_API_KEY=...
ANGELONE_TOTP_SECRET=...
```

## Running the Bots

### 1. Unified Multi-Bot (Recommended)
Monitors both BTCUSD and Nifty50 simultaneously.
```bash
python multi_rsi_divergence_bot.py
```

### 2. Individual Bots
```bash
python rsi_divergence_bot.py          # BTCUSD only
python nifty50_rsi_divergence_bot.py # Nifty50 only
```

## Alert Types
| Alert | Condition |
|---|---|
| 🔴 Bearish Divergence | Price higher high, RSI lower high |
| 🟢 Bullish Divergence | Price lower low, RSI higher low  |
| 🔻 SELL Confirmed | Price breaks below divergence low + RSI < 60 |
| ✅ BUY Confirmed  | Price breaks above divergence high + RSI > 40 |
