"""
╔══════════════════════════════════════════════════════════════════════╗
║  Multi-RSI Divergence Alert Bot  ·  Delta & Angel One Unified        ║
║  Replicates: RSI with Close & Tail Divergences v1.0 (Pine Script)  ║
╚══════════════════════════════════════════════════════════════════════╝

Setup:
  1. pip install requests pandas pandas_ta python-dotenv smartapi-python pyotp
  2. Fill your .env with:
       TELEGRAM_TOKEN=...
       TELEGRAM_CHAT_ID=...
       DELTA_API_KEY=...
       ANGELONE_CLIENT_ID=...
       ANGELONE_PASSWORD=...
       ANGELONE_API_KEY=...
       ANGELONE_TOTP_SECRET=...
  3. python multi_rsi_divergence_bot.py
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
#  CONFIGURATION & GLOBALS
# ─────────────────────────────────────────────────────────────────────

load_dotenv()

# ── Credentials ───────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN",   "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Delta Exchange
DELTA_BASE_URL   = "https://api.india.delta.exchange"
DELTA_API_KEY    = os.getenv("DELTA_API_KEY", "")

# Angel One
AONE_CLIENT_ID   = os.getenv("ANGELONE_CLIENT_ID", "")
AONE_PASSWORD    = os.getenv("ANGELONE_PASSWORD",  "")
AONE_API_KEY     = os.getenv("ANGELONE_API_KEY",   "")
AONE_TOTP_SECRET = os.getenv("ANGELONE_TOTP_SECRET", "")

# ── General Strategy Settings ──────────────────────────────────────────
RSI_LENGTH    = 14
LOOKBACK      = 7
RSI_BUY_MIN   = 40
RSI_SELL_MAX  = 60
CANDLES_FETCH = 100
DRY_RUN       = False

# ── Target Symbols (Expandable) ──────────────────────────────────────────
# We now define each Symbol + Timeframe combination as a target.
TARGETS = [
    # --- BTCUSD (Delta Exchange) ---
    {"symbol": "BTCUSD", "exchange": "Delta", "interval": "5m",    "gated": False, "label": "$"},
    {"symbol": "BTCUSD", "exchange": "Delta", "interval": "15m",   "gated": False, "label": "$"},
    {"symbol": "BTCUSD", "exchange": "Delta", "interval": "30m",   "gated": False, "label": "$"},
    {"symbol": "BTCUSD", "exchange": "Delta", "interval": "1h",    "gated": False, "label": "$"},
    {"symbol": "BTCUSD", "exchange": "Delta", "interval": "4h",    "gated": False, "label": "$"},

    # --- Nifty50 (Angel One) ---
    # Note: Angel One does NOT support native 4h for Index candles.
    {"symbol": "Nifty50", "exchange": "AngelOne", "interval": "FIVE_MINUTE",    "token": "99926000", "gated": True, "label": "₹", "display_tf": "5m"},
    {"symbol": "Nifty50", "exchange": "AngelOne", "interval": "FIFTEEN_MINUTE", "token": "99926000", "gated": True, "label": "₹", "display_tf": "15m"},
    {"symbol": "Nifty50", "exchange": "AngelOne", "interval": "THIRTY_MINUTE",  "token": "99926000", "gated": True, "label": "₹", "display_tf": "30m"},
    {"symbol": "Nifty50", "exchange": "AngelOne", "interval": "ONE_HOUR",       "token": "99926000", "gated": True, "label": "₹", "display_tf": "1h"},
]

# ─────────────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("MULTI-DIV-BOT")

# ─────────────────────────────────────────────────────────────────────
#  UTILITIES
# ─────────────────────────────────────────────────────────────────────

def is_market_open() -> bool:
    """Checks if current time is within Indian Stock Market hours (09:15-15:30 IST, Mon-Fri)."""
    utc_now = datetime.now(timezone.utc)
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    if ist_now.weekday() > 4: return False
    market_start = ist_now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_end   = ist_now.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_start <= ist_now <= market_end

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
    except Exception as exc:
        log.error(f"Telegram send failed: {exc}")

# ─────────────────────────────────────────────────────────────────────
#  FETCHERS
# ─────────────────────────────────────────────────────────────────────

def fetch_delta_candles(symbol: str, resolution: str, count: int) -> pd.DataFrame:
    """Fetch candles from Delta Exchange India."""
    res_secs = {
        "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
        "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600, "1d": 86400
    }
    secs = res_secs.get(resolution, 300)
    now = int(time.time())
    start = now - secs * (count + 10)
    
    url = f"{DELTA_BASE_URL}/v2/history/candles"
    params = {"symbol": symbol, "resolution": resolution, "start": start, "end": now}
    headers = {"api-key": DELTA_API_KEY} if DELTA_API_KEY else {}

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        result = resp.json().get("result", [])
        if not result: return pd.DataFrame()
        
        df = pd.DataFrame(result)
        df.rename(columns={"t": "time", "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"}, inplace=True)
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["time"] = pd.to_numeric(df["time"], errors="coerce")
        df.sort_values("time", inplace=True)
        
        # Drop the live candle (last row) to ensure we only look at closed bars
        if len(df) > 1: df = df.iloc[:-1].copy()
        return df.tail(count).reset_index(drop=True)
    except Exception as exc:
        log.error(f"Delta fetch error ({symbol} @ {resolution}): {exc}")
        return pd.DataFrame()

def fetch_angel_candles(smart_api, token: str, interval: str, count: int) -> pd.DataFrame:
    """Fetch candles from Angel One."""
    res_mins = {
        "ONE_MINUTE": 1, "FIVE_MINUTE": 5, "FIFTEEN_MINUTE": 15,
        "THIRTY_MINUTE": 30, "ONE_HOUR": 60, "ONE_DAY": 1440
    }
    m = res_mins.get(interval, 5)
    
    now_ist = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
    from_ist = now_ist - timedelta(minutes=(count + 30) * m)
    
    historic_param = {
        "exchange": "NSE",
        "symboltoken": token,
        "interval": interval,
        "fromdate": from_ist.strftime("%Y-%m-%d %H:%M"),
        "todate": now_ist.strftime("%Y-%m-%d %H:%M")
    }
    try:
        resp = smart_api.getCandleData(historic_param)
        if not resp.get('status'):
            log.error(f"AngelOne fetch error ({interval}): {resp.get('message')}")
            return pd.DataFrame()
        
        data = resp.get('data', [])
        if not data: return pd.DataFrame()
        
        df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"]).apply(lambda x: int(x.timestamp()))
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df.sort_values("time", inplace=True)
        
        # Drop current open candle
        if len(df) > 1: df = df.iloc[:-1].copy()
        return df.tail(count).reset_index(drop=True)
    except Exception as exc:
        log.error(f"AngelOne fetch exception ({interval}): {exc}")
        return pd.DataFrame()

# ─────────────────────────────────────────────────────────────────────
#  STRATEGY LOGIC
# ─────────────────────────────────────────────────────────────────────

def add_rsi(df: pd.DataFrame, length: int = 14) -> pd.DataFrame:
    df = df.copy()
    df["rsi"] = ta.rsi(df["close"], length=length)
    return df

def compute_divergences(df: pd.DataFrame, lookback: int = 7) -> pd.DataFrame:
    df = df.copy()
    n = len(df)
    max_rsi_arr, min_rsi_arr = [float("nan")] * n, [float("nan")] * n
    max_close_arr, min_close_arr = [float("nan")] * n, [float("nan")] * n
    divbear, divbull = [False] * n, [False] * n

    for i in range(n):
        rsi_val, close_val = df["rsi"].iloc[i], df["close"].iloc[i]
        if math.isnan(rsi_val): continue

        start_w = max(0, i - lookback + 1)
        window_rsi = df["rsi"].iloc[start_w : i + 1].dropna()
        if len(window_rsi) == 0:
            max_rsi_arr[i] = min_rsi_arr[i] = rsi_val
            max_close_arr[i] = min_close_arr[i] = close_val
            continue

        hb0, lb0 = (rsi_val == window_rsi.max()), (rsi_val == window_rsi.min())

        # Update Highs
        if hb0: max_rsi_arr[i], max_close_arr[i] = rsi_val, close_val
        elif i > 0 and not math.isnan(max_rsi_arr[i-1]):
            max_rsi_arr[i], max_close_arr[i] = max_rsi_arr[i-1], max_close_arr[i-1]
        else: max_rsi_arr[i], max_close_arr[i] = rsi_val, close_val
        if close_val > max_close_arr[i]: max_close_arr[i] = close_val
        if rsi_val > max_rsi_arr[i]: max_rsi_arr[i] = rsi_val

        # Update Lows
        if lb0: min_rsi_arr[i], min_close_arr[i] = rsi_val, close_val
        elif i > 0 and not math.isnan(min_rsi_arr[i-1]):
            min_rsi_arr[i], min_close_arr[i] = min_rsi_arr[i-1], min_close_arr[i-1]
        else: min_rsi_arr[i], min_close_arr[i] = rsi_val, close_val
        if close_val < min_close_arr[i]: min_close_arr[i] = close_val
        if rsi_val < min_rsi_arr[i]: min_rsi_arr[i] = rsi_val

        # Divergence check (at least 2 bars)
        if i >= 2:
            mc1, mc2, rsi1, maxr = max_close_arr[i-1], max_close_arr[i-2], df["rsi"].iloc[i-1], max_rsi_arr[i]
            if not any(math.isnan(v) for v in [mc1, mc2, rsi1, maxr]):
                if mc1 > mc2 and rsi1 < maxr and rsi_val <= rsi1: divbear[i] = True
            
            minc1, minc2, mr1 = min_close_arr[i-1], min_close_arr[i-2], min_rsi_arr[i]
            if not any(math.isnan(v) for v in [minc1, minc2, mr1]):
                if minc1 < minc2 and rsi1 > mr1 and rsi_val >= rsi1: divbull[i] = True

    df["divbear_close"], df["divbull_close"] = divbear, divbull
    return df

class TriggerTracker:
    def __init__(self):
        self.bull_trigger = self.bear_trigger = None
    def update(self, row: pd.Series) -> dict:
        sigs = {"divbear": False, "divbull": False, "buy": False, "sell": False}
        if row["divbear_close"]:
            sigs["divbear"], self.bear_trigger, self.bull_trigger = True, row["low"], None
        elif row["divbull_close"]:
            sigs["divbull"], self.bull_trigger, self.bear_trigger = True, row["high"], None
        
        bull_broke = (self.bull_trigger is not None) and (row["high"] >= self.bull_trigger)
        bear_broke = (self.bear_trigger is not None) and (row["low"] <= self.bear_trigger)
        
        if bull_broke and row["rsi"] > RSI_BUY_MIN: sigs["buy"] = True
        if bear_broke and row["rsi"] < RSI_SELL_MAX: sigs["sell"] = True
        if bull_broke: self.bull_trigger = None
        if bear_broke: self.bear_trigger = None
        return sigs

# ─────────────────────────────────────────────────────────────────────
#  UI / ALERTS
# ─────────────────────────────────────────────────────────────────────

def build_alert(tcfg: dict, sig_type: str, row: pd.Series) -> str:
    # Use IST for all display timestamps
    ts_ist = datetime.fromtimestamp(row["time"], tz=timezone.utc) + timedelta(hours=5, minutes=30)
    ts_str = ts_ist.strftime("%Y-%m-%d %H:%M IST")
    symbol, px, rsi, lb = tcfg["symbol"], row["close"], row["rsi"], tcfg["label"]
    tf_display = tcfg.get("display_tf", tcfg["interval"])
    
    icons = {"divbear": "🔴 Bearish Div", "divbull": "🟢 Bullish Div", "buy": "✅ BUY CONFIRMED ▲", "sell": "🔻 SELL CONFIRMED ▼"}
    notes = {
        "divbear": "⚠️ Watch for break below low",
        "divbull": "⚠️ Watch for break above high",
        "buy": "Candle broke above high with RSI > 40",
        "sell": "Candle broke below low with RSI < 60"
    }
    
    return (
        f"<b>{icons.get(sig_type)}</b>\n"
        f"Symbol : {symbol}  |  TF : {tf_display}\n"
        f"Time   : {ts_str}\n"
        f"Close  : {lb}{px:.2f}  |  RSI : {rsi:.2f}\n"
        f"{notes.get(sig_type)}"
    )

# ─────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────

def main():
    log.info("═" * 60)
    log.info("  Unified Multi-RSI Divergence Bot (Multi-Timeframe)")
    log.info("═" * 60)

    # State tracking keyed by (Symbol, Interval)
    trackers = {}
    last_bars = {}
    
    for t in TARGETS:
        key = f"{t['symbol']}_{t['interval']}"
        trackers[key] = TriggerTracker()
        last_bars[key] = None
    
    # Session handling for Angel One
    aone_session = None
    if any(t["exchange"] == "AngelOne" for t in TARGETS):
        try:
            smart = SmartConnect(api_key=AONE_API_KEY)
            totp = pyotp.TOTP(AONE_TOTP_SECRET).now()
            res = smart.generateSession(AONE_CLIENT_ID, AONE_PASSWORD, totp)
            if res['status']:
                aone_session = smart
                log.info("Angel One login successful")
            else:
                log.error(f"Angel One login failed: {res['message']}")
        except Exception as e:
            log.error(f"Angel One auth error: {e}")

    while True:
        cycle_start = time.time()
        
        for tcfg in TARGETS:
            sym = tcfg["symbol"]
            exch = tcfg["exchange"]
            interval = tcfg["interval"]
            key = f"{sym}_{interval}"
            
            # 1. Market Hours Check
            if tcfg["gated"] and not is_market_open():
                continue
                
            # 2. Fetch
            if exch == "Delta":
                df = fetch_delta_candles(sym, interval, CANDLES_FETCH)
            elif exch == "AngelOne" and aone_session:
                df = fetch_angel_candles(aone_session, tcfg["token"], interval, CANDLES_FETCH)
            else:
                continue

            if df.empty or len(df) < RSI_LENGTH + LOOKBACK + 5:
                continue

            # 3. Process
            df = add_rsi(df, RSI_LENGTH)
            df = compute_divergences(df, LOOKBACK)
            latest = df.iloc[-1]
            
            if latest["time"] == last_bars[key]:
                continue
            
            last_bars[key] = latest["time"]
            ts_ist = datetime.fromtimestamp(latest["time"], tz=timezone.utc) + timedelta(hours=5, minutes=30)
            tf_disp = tcfg.get("display_tf", interval)
            log.info(f"[{sym} @ {tf_disp}] bar {ts_ist.strftime('%H:%M')} px={latest['close']:.2f} rsi={latest['rsi']:.2f}")

            # 4. Signals
            sigs = trackers[key].update(latest)
            for s_type, fired in sigs.items():
                if fired:
                    msg = build_alert(tcfg, s_type, latest)
                    log.info(f"[{sym} @ {tf_disp}] SIGNAL: {s_type.upper()}")
                    send_telegram(msg)

        # Wait ~60s per loop cycle
        time.sleep(max(10, 60 - (time.time() - cycle_start)))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Stopped by user.")
    except Exception as e:
        log.exception(f"Fatal crash: {e}")
