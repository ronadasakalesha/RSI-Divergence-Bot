"""
╔══════════════════════════════════════════════════════════════════════╗
║  Nifty50 RSI Divergence Alert Bot  ·  Angel One SmartAPI             ║
║  Replicates: RSI with Close & Tail Divergences v1.0 (Pine Script)  ║
╚══════════════════════════════════════════════════════════════════════╝

Setup:
  1. pip install smartapi-python pyotp requests pandas pandas_ta python-dotenv
  2. Create a .env file alongside this script with:
       TELEGRAM_TOKEN=your_bot_token
       TELEGRAM_CHAT_ID=your_chat_id
       ANGELONE_CLIENT_ID=your_client_id
       ANGELONE_PASSWORD=your_password
       ANGELONE_API_KEY=your_api_key
       ANGELONE_TOTP_SECRET=your_totp_secret
  3. python nifty50_rsi_divergence_bot.py

How to get Angel One credentials:
  • API Key   → https://smartapi.angelbroking.com/
  • TOTP Key  → Enable 2FA on Angel One app, copy the key shown during setup.
"""

import os
import time
import math
import logging
import requests
import pyotp
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from SmartApi import SmartConnect

# ─────────────────────────────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────────────────────────────

load_dotenv()

# ── Telegram ──────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN",   "YOUR_BOT_TOKEN_HERE")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID_HERE")

# ── Angel One ─────────────────────────────────────────────────────────
CLIENT_ID   = os.getenv("ANGELONE_CLIENT_ID", "")
PASSWORD    = os.getenv("ANGELONE_PASSWORD",  "")
API_KEY     = os.getenv("ANGELONE_API_KEY",   "")
TOTP_SECRET = os.getenv("ANGELONE_TOTP_SECRET", "")

# ── Strategy parameters ───────────────────────────────────────────────
SYMBOL        = "Nifty50"
TOKEN         = "99926000"      # Nifty 50 Index token on NSE
EXCHANGE      = "NSE"
INTERVAL      = "FIVE_MINUTE"   # Angel One intervals: ONE_MINUTE, FIVE_MINUTE, ...
RSI_LENGTH    = 14
OVERBOUGHT    = 70
OVERSOLD      = 30
LOOKBACK      = 7
RSI_BUY_MIN   = 40
RSI_SELL_MAX  = 60
CANDLES_FETCH = 100
POLL_SECONDS  = 60             # Check every minute for market status/candle closure
DRY_RUN       = False

# ─────────────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("NIFTY-DIV-BOT")

# ─────────────────────────────────────────────────────────────────────
#  MARKET HOURS LOGIC (IST)
# ─────────────────────────────────────────────────────────────────────

def is_market_open() -> bool:
    """Checks if current time is within Indian Stock Market hours (09:15-15:30 IST, Mon-Fri)."""
    # Get current time in IST
    utc_now = datetime.now(timezone.utc)
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    
    # Check weekday (0=Monday, 6=Sunday)
    day = ist_now.weekday()
    if day > 4:  # Sat or Sun
        return False
        
    # Check time (09:15 to 15:30)
    market_start = ist_now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_end   = ist_now.replace(hour=15, minute=30, second=0, microsecond=0)
    
    return market_start <= ist_now <= market_end

# ─────────────────────────────────────────────────────────────────────
#  TELEGRAM SENDER
# ─────────────────────────────────────────────────────────────────────

def send_telegram(message: str) -> None:
    if DRY_RUN:
        log.info(f"[DRY-RUN] Telegram → {message}")
        return
    url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        resp = requests.post(url, data=data, timeout=10)
        if resp.status_code != 200:
            log.warning(f"Telegram error {resp.status_code}: {resp.text}")
        else:
            log.info("Telegram alert sent ✓")
    except Exception as exc:
        log.error(f"Telegram send failed: {exc}")

# ─────────────────────────────────────────────────────────────────────
#  ANGEL ONE — AUTHENTICATION & FETCH
# ─────────────────────────────────────────────────────────────────────

def login_angel_one():
    """Initializes and logs into SmartConnect."""
    try:
        smart_api = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_SECRET).now()
        data = smart_api.generateSession(CLIENT_ID, PASSWORD, totp)
        
        if data['status']:
            log.info("Angel One login successful")
            return smart_api
        else:
            log.error(f"Angel One login failed: {data['message']}")
            return None
    except Exception as exc:
        log.error(f"Angel One connection error: {exc}")
        return None

def fetch_candles(smart_api, token: str, exchange: str, interval: str, count: int) -> pd.DataFrame:
    """Fetch candles from Angel One."""
    # Compute fromdate/todate (with buffer)
    # Angel One format: "yyyy-MM-dd HH:mm"
    now_ist = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
    # Fetch a bit more to be safe
    from_ist = now_ist - timedelta(minutes=(count + 20) * 5)
    
    from_date_str = from_ist.strftime("%Y-%m-%d %H:%M")
    to_date_str   = now_ist.strftime("%Y-%m-%d %H:%M")
    
    try:
        historic_param = {
            "exchange": exchange,
            "symboltoken": token,
            "interval": interval,
            "fromdate": from_date_str,
            "todate": to_date_str
        }
        resp = smart_api.getCandleData(historic_param)
        
        if not resp.get('status'):
            log.error(f"Fetch candles error: {resp.get('message')}")
            return pd.DataFrame()
        
        data = resp.get('data', [])
        if not data:
            return pd.DataFrame()
            
        # Angel One returns: [time, open, high, low, close, volume]
        df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])
        
        # Convert time to unix timestamp for consistency with Pine Script logic port
        # Input format: '2023-01-01T09:15:00+05:30' or similar
        df["time"] = pd.to_datetime(df["time"]).apply(lambda x: int(x.timestamp()))
        
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            
        df.sort_values("time", inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        # Drop current open candle if confirmed closed logic is needed
        # Angel One historical data for 5m might include the live one at the end
        if len(df) > 1:
            df = df.iloc[:-1].copy()
            
        return df.tail(count).reset_index(drop=True)
        
    except Exception as exc:
        log.error(f"Candle fetch exception: {exc}")
        return pd.DataFrame()

# ─────────────────────────────────────────────────────────────────────
#  STRATEGY LOGIC (PORTE FROM BTCUSD BOT)
# ─────────────────────────────────────────────────────────────────────

def add_rsi(df: pd.DataFrame, length: int = 14) -> pd.DataFrame:
    df = df.copy()
    df["rsi"] = ta.rsi(df["close"], length=length)
    return df

def compute_divergences(df: pd.DataFrame, lookback: int = 7) -> pd.DataFrame:
    df = df.copy()
    n  = len(df)
    max_rsi_arr   = [float("nan")] * n
    min_rsi_arr   = [float("nan")] * n
    max_close_arr = [float("nan")] * n
    min_close_arr = [float("nan")] * n
    divbear       = [False] * n
    divbull       = [False] * n

    for i in range(n):
        rsi_val   = df["rsi"].iloc[i]
        close_val = df["close"].iloc[i]

        if math.isnan(rsi_val):
            continue

        start_w = max(0, i - lookback + 1)
        window_rsi = df["rsi"].iloc[start_w : i + 1].dropna()

        if len(window_rsi) == 0:
            max_rsi_arr[i]   = rsi_val
            min_rsi_arr[i]   = rsi_val
            max_close_arr[i] = close_val
            min_close_arr[i] = close_val
            continue

        hb_is_zero = (rsi_val == window_rsi.max())
        lb_is_zero = (rsi_val == window_rsi.min())

        if hb_is_zero:
            max_rsi_arr[i]   = rsi_val
            max_close_arr[i] = close_val
        elif i == 0 or math.isnan(max_rsi_arr[i - 1]):
            max_rsi_arr[i]   = rsi_val
            max_close_arr[i] = close_val
        else:
            max_rsi_arr[i]   = max_rsi_arr[i - 1]
            max_close_arr[i] = max_close_arr[i - 1]

        if close_val > max_close_arr[i]:
            max_close_arr[i] = close_val
        if rsi_val > max_rsi_arr[i]:
            max_rsi_arr[i] = rsi_val

        if lb_is_zero:
            min_rsi_arr[i]   = rsi_val
            min_close_arr[i] = close_val
        elif i == 0 or math.isnan(min_rsi_arr[i - 1]):
            min_rsi_arr[i]   = rsi_val
            min_close_arr[i] = close_val
        else:
            min_rsi_arr[i]   = min_rsi_arr[i - 1]
            min_close_arr[i] = min_close_arr[i - 1]

        if close_val < min_close_arr[i]:
            min_close_arr[i] = close_val
        if rsi_val < min_rsi_arr[i]:
            min_rsi_arr[i] = rsi_val

        if i >= 2:
            mc1   = max_close_arr[i - 1]
            mc2   = max_close_arr[i - 2]
            rsi1  = df["rsi"].iloc[i - 1]
            maxr  = max_rsi_arr[i]

            if not any(math.isnan(v) for v in [mc1, mc2, rsi1, maxr]):
                if mc1 > mc2 and rsi1 < maxr and rsi_val <= rsi1:
                    divbear[i] = True

                mr1  = min_rsi_arr[i]
                minc1 = min_close_arr[i - 1]
                minc2 = min_close_arr[i - 2]
                if not any(math.isnan(v) for v in [minc1, minc2, mr1]):
                    if minc1 < minc2 and rsi1 > mr1 and rsi_val >= rsi1:
                        divbull[i] = True

    df["max_rsi"]      = max_rsi_arr
    df["min_rsi"]      = min_rsi_arr
    df["max_close"]    = max_close_arr
    df["min_close"]    = min_close_arr
    df["divbear_close"] = divbear
    df["divbull_close"] = divbull
    return df

class TriggerTracker:
    def __init__(self):
        self.bull_trigger: float | None = None
        self.bear_trigger: float | None = None

    def update(self, row: pd.Series) -> dict:
        signals = {"divbear": False, "divbull": False, "buy": False, "sell": False}
        rsi_val  = row["rsi"]
        high_val = row["high"]
        low_val  = row["low"]

        if row["divbear_close"]:
            signals["divbear"]    = True
            self.bear_trigger     = low_val
            self.bull_trigger     = None
        elif row["divbull_close"]:
            signals["divbull"]    = True
            self.bull_trigger     = high_val
            self.bear_trigger     = None

        bull_broke = (self.bull_trigger is not None) and (high_val >= self.bull_trigger)
        bear_broke = (self.bear_trigger is not None) and (low_val  <= self.bear_trigger)

        if bull_broke and rsi_val > RSI_BUY_MIN:
            signals["buy"] = True
        if bear_broke and rsi_val < RSI_SELL_MAX:
            signals["sell"] = True

        if bull_broke: self.bull_trigger = None
        if bear_broke: self.bear_trigger = None
        return signals

# ─────────────────────────────────────────────────────────────────────
#  ALERT MESSAGE BUILDER
# ─────────────────────────────────────────────────────────────────────

def build_alert(signal_type: str, row: pd.Series) -> str:
    # Convert ts to IST for display
    ts_ist = datetime.fromtimestamp(row["time"], tz=timezone.utc) + timedelta(hours=5, minutes=30)
    ts_str = ts_ist.strftime("%Y-%m-%d %H:%M IST")
    rsi = f"{row['rsi']:.2f}"
    px  = f"{row['close']:.2f}"

    templates = {
        "divbear": (
            "🔴 <b>Bearish RSI Divergence Detected</b>\n"
            f"Symbol : {SYMBOL}  |  TF : 5m\n"
            f"Time   : {ts_str}\n"
            f"Close  : ₹{px}  |  RSI : {rsi}\n"
            "⚠️ Watch for SELL break below divergence low"
        ),
        "divbull": (
            "🟢 <b>Bullish RSI Divergence Detected</b>\n"
            f"Symbol : {SYMBOL}  |  TF : 5m\n"
            f"Time   : {ts_str}\n"
            f"Close  : ₹{px}  |  RSI : {rsi}\n"
            "⚠️ Watch for BUY break above divergence high"
        ),
        "buy": (
            "✅ <b>BUY CONFIRMED ▲</b>\n"
            f"Symbol : {SYMBOL}  |  TF : 5m\n"
            f"Time   : {ts_str}\n"
            f"Close  : ₹{px}  |  RSI : {rsi}\n"
            "Candle broke above divergence high with RSI > 40"
        ),
        "sell": (
            "🔻 <b>SELL CONFIRMED ▼</b>\n"
            f"Symbol : {SYMBOL}  |  TF : 5m\n"
            f"Time   : {ts_str}\n"
            f"Close  : ₹{px}  |  RSI : {rsi}\n"
            "Candle broke below divergence low with RSI < 60"
        ),
    }
    return templates.get(signal_type, f"Signal: {signal_type}")

# ─────────────────────────────────────────────────────────────────────
#  MAIN LOOP
# ─────────────────────────────────────────────────────────────────────

def main():
    log.info("═" * 60)
    log.info("  Nifty50 RSI Divergence Bot  —  Angel One")
    log.info(f"  Symbol: {SYMBOL}  |  TF: 5m  |  DRY_RUN: {DRY_RUN}")
    log.info("═" * 60)

    if TELEGRAM_TOKEN == "YOUR_BOT_TOKEN_HERE" and not DRY_RUN:
        log.error("Set TELEGRAM_TOKEN in your .env file or enable DRY_RUN=True")
        return

    session = login_angel_one()
    if not session:
        log.error("Failed to initialize Angel One session. Exiting.")
        return

    tracker       = TriggerTracker()
    last_bar_time = None

    while True:
        try:
            if not is_market_open():
                log.info("Market is closed (IST). Sleeping for 1 minute.")
                time.sleep(60)
                continue

            # Check if session is still valid, if not, re-login
            # (Simple check: attempt a trivial call or just rely on error handling)
            
            df = fetch_candles(session, TOKEN, EXCHANGE, INTERVAL, CANDLES_FETCH)

            if df.empty or len(df) < RSI_LENGTH + LOOKBACK + 5:
                log.warning("Not enough candles or fetch failed — waiting 30s")
                time.sleep(30)
                continue

            df = add_rsi(df, RSI_LENGTH)
            df = compute_divergences(df, LOOKBACK)

            latest = df.iloc[-1]
            bar_time = latest["time"]

            if bar_time == last_bar_time:
                # Still on the same candle, wait for next check
                time.sleep(30)
                continue

            last_bar_time = bar_time
            ts_ist = datetime.fromtimestamp(bar_time, tz=timezone.utc) + timedelta(hours=5, minutes=30)
            log.info(
                f"Bar {ts_ist.strftime('%H:%M')}  close={latest['close']:.2f}  "
                f"rsi={latest['rsi']:.2f}  "
                f"divbear={latest['divbear_close']}  divbull={latest['divbull_close']}"
            )

            signals = tracker.update(latest)
            for sig_type, fired in signals.items():
                if fired:
                    msg = build_alert(sig_type, latest)
                    log.info(f"SIGNAL → {sig_type.upper()}")
                    send_telegram(msg)

        except KeyboardInterrupt:
            log.info("Bot stopped by user.")
            break
        except Exception as exc:
            log.exception(f"Unexpected error in main loop: {exc}")
            # Try to re-login if session is lost
            log.info("Attempting session recovery...")
            session = login_angel_one()
            time.sleep(30)

        time.sleep(30)

if __name__ == "__main__":
    main()
