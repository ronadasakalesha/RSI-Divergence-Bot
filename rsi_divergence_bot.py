"""
╔══════════════════════════════════════════════════════════════════════╗
║  RSI Divergence Alert Bot  ·  Delta Exchange India  ·  BTCUSD 5M  ║
║  Replicates: RSI with Close & Tail Divergences v1.0 (Pine Script)  ║
╚══════════════════════════════════════════════════════════════════════╝

Setup:
  1. pip install requests pandas pandas_ta python-dotenv
  2. Create a .env file alongside this script with:
       TELEGRAM_TOKEN=your_bot_token_from_BotFather
       TELEGRAM_CHAT_ID=your_chat_id
       DELTA_API_KEY=your_delta_api_key        (optional, for private data)
       DELTA_API_SECRET=your_delta_api_secret  (optional)
  3. python rsi_divergence_bot.py

How to get Telegram credentials:
  • BOT TOKEN  → Chat with @BotFather on Telegram → /newbot
  • CHAT ID    → Chat with @userinfobot on Telegram
"""

import os
import time
import math
import logging
import requests
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timezone
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────────────
#  CONFIGURATION  (edit here or put secrets in .env)
# ─────────────────────────────────────────────────────────────────────

load_dotenv()

# ── Telegram ──────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN",   "YOUR_BOT_TOKEN_HERE")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID_HERE")

# ── Delta Exchange India ──────────────────────────────────────────────
DELTA_BASE_URL   = "https://api.india.delta.exchange"
DELTA_API_KEY    = os.getenv("DELTA_API_KEY",    "")   # optional for public OHLCV
DELTA_API_SECRET = os.getenv("DELTA_API_SECRET", "")   # optional

# ── Strategy parameters (mirrors Pine Script inputs) ──────────────────
SYMBOL        = "BTCUSD"        # Delta Exchange product symbol
RESOLUTION    = "5m"            # 1m 3m 5m 15m 30m 1h 4h 1d …
RSI_LENGTH    = 14              # Pine: len = 14
OVERBOUGHT    = 70              # Pine: oBought = 70
OVERSOLD      = 30              # Pine: oSold  = 30
LOOKBACK      = 7               # Pine: calcBars = 7  (divergence lookback bars)
RSI_BUY_MIN   = 40             # Pine: rsi > 40 filter on BUY confirmation
RSI_SELL_MAX  = 60             # Pine: rsi < 60 filter on SELL confirmation
CANDLES_FETCH = 100            # How many recent candles to fetch each cycle
POLL_SECONDS  = 300            # 300 s = 5 min  (run once per completed candle)
DRY_RUN       = False          # True → print alerts, don't send to Telegram

# ─────────────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("RSI-DIV-BOT")

# ─────────────────────────────────────────────────────────────────────
#  TELEGRAM SENDER
# ─────────────────────────────────────────────────────────────────────

def send_telegram(message: str) -> None:
    """Send a plain-text message to Telegram."""
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
#  DELTA EXCHANGE INDIA — FETCH CANDLES
# ─────────────────────────────────────────────────────────────────────

def fetch_candles(symbol: str, resolution: str, count: int) -> pd.DataFrame:
    """
    Fetch the last `count` closed OHLCV candles from Delta Exchange India.
    Returns a DataFrame with columns: [time, open, high, low, close, volume]
    sorted ascending (oldest → newest).
    """
    # How many seconds per candle → for start timestamp calculation
    resolution_seconds = {
        "1m": 60, "3m": 180, "5m": 300, "15m": 900,
        "30m": 1800, "1h": 3600, "2h": 7200, "4h": 14400,
        "6h": 21600, "1d": 86400, "7d": 604800,
    }
    secs = resolution_seconds.get(resolution, 300)

    now   = int(time.time())
    start = now - secs * (count + 5)   # a little buffer

    url    = f"{DELTA_BASE_URL}/v2/history/candles"
    params = {
        "symbol":     symbol,
        "resolution": resolution,
        "start":      start,
        "end":        now,
    }
    headers = {}
    if DELTA_API_KEY:
        headers["api-key"] = DELTA_API_KEY

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.error(f"Candle fetch error: {exc}")
        return pd.DataFrame()

    # Delta response: {"success": true, "result": [{time, open, high, low, close, volume}, ...]}
    result = data.get("result", [])
    if not result:
        log.warning("Empty candle response from Delta Exchange")
        return pd.DataFrame()

    df = pd.DataFrame(result)
    df.rename(columns={"t": "time", "o": "open", "h": "high",
                        "l": "low",  "c": "close", "v": "volume"}, errors="ignore", inplace=True)

    # Convert column names from Delta's actual format (they may already be full names)
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "time" in df.columns:
        df["time"] = pd.to_numeric(df["time"], errors="coerce")
        df.sort_values("time", inplace=True)
        df.reset_index(drop=True, inplace=True)

    # Drop the very last candle if it may still be open
    # (barstate.isconfirmed equivalent — only use closed bars)
    if len(df) > 1:
        df = df.iloc[:-1].copy()

    return df.tail(count).reset_index(drop=True)

# ─────────────────────────────────────────────────────────────────────
#  RSI CALCULATION
# ─────────────────────────────────────────────────────────────────────

def add_rsi(df: pd.DataFrame, length: int = 14) -> pd.DataFrame:
    """Appends an 'rsi' column using Wilder's smoothing (same as Pine Script)."""
    df = df.copy()
    df["rsi"] = ta.rsi(df["close"], length=length)
    return df

# ─────────────────────────────────────────────────────────────────────
#  DIVERGENCE DETECTION
#  Exact port of Pine Script logic:
#    hb = math.abs(ta.highestbars(rsi, calcBars))
#    lb = math.abs(ta.lowestbars(rsi, calcBars))
#    max_rsi := hb == 0 ? rsi : max_rsi[1]
#    min_rsi := lb == 0 ? rsi : min_rsi[1]
#    max_close := hb == 0 ? close : max_close[1]
#    min_close := lb == 0 ? close : min_close[1]
#    divbear_close := max_close[1] > max_close[2] and rsi[1] < max_rsi and rsi <= rsi[1]
#    divbull_close := min_close[1] < min_close[2] and rsi[1] > min_rsi and rsi >= rsi[1]
# ─────────────────────────────────────────────────────────────────────

def compute_divergences(df: pd.DataFrame, lookback: int = 7) -> pd.DataFrame:
    """
    Replicates the Pine Script stateful max/min tracking + divergence detection.
    Adds columns: max_rsi, min_rsi, max_close, min_close,
                  divbear_close, divbull_close
    """
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
            max_rsi_arr[i]   = float("nan")
            min_rsi_arr[i]   = float("nan")
            max_close_arr[i] = float("nan")
            min_close_arr[i] = float("nan")
            continue

        # Window for highestbars / lowestbars
        start_w = max(0, i - lookback + 1)
        window_rsi = df["rsi"].iloc[start_w : i + 1].dropna()

        if len(window_rsi) == 0:
            max_rsi_arr[i]   = rsi_val
            min_rsi_arr[i]   = rsi_val
            max_close_arr[i] = close_val
            min_close_arr[i] = close_val
            continue

        # hb == 0 means current bar IS the highest in lookback window
        hb_is_zero = (rsi_val == window_rsi.max())
        lb_is_zero = (rsi_val == window_rsi.min())

        # max_rsi
        if hb_is_zero:
            max_rsi_arr[i]   = rsi_val
            max_close_arr[i] = close_val
        elif i == 0 or math.isnan(max_rsi_arr[i - 1]):
            max_rsi_arr[i]   = rsi_val
            max_close_arr[i] = close_val
        else:
            max_rsi_arr[i]   = max_rsi_arr[i - 1]
            max_close_arr[i] = max_close_arr[i - 1]

        # Update max if current bar exceeds stored max (Pine's if close > max_close block)
        if close_val > max_close_arr[i]:
            max_close_arr[i] = close_val
        if rsi_val > max_rsi_arr[i]:
            max_rsi_arr[i] = rsi_val

        # min_rsi
        if lb_is_zero:
            min_rsi_arr[i]   = rsi_val
            min_close_arr[i] = close_val
        elif i == 0 or math.isnan(min_rsi_arr[i - 1]):
            min_rsi_arr[i]   = rsi_val
            min_close_arr[i] = close_val
        else:
            min_rsi_arr[i]   = min_rsi_arr[i - 1]
            min_close_arr[i] = min_close_arr[i - 1]

        # Update min if current bar is below stored min
        if close_val < min_close_arr[i]:
            min_close_arr[i] = close_val
        if rsi_val < min_rsi_arr[i]:
            min_rsi_arr[i] = rsi_val

        # ── Divergence conditions (evaluated AFTER state update, like Pine) ──
        # divbear_close := max_close[1] > max_close[2] and rsi[1] < max_rsi and rsi <= rsi[1]
        if i >= 2:
            mc1   = max_close_arr[i - 1]
            mc2   = max_close_arr[i - 2]
            rsi1  = df["rsi"].iloc[i - 1]
            maxr  = max_rsi_arr[i]         # max_rsi at bar i (Pine: max_rsi without lag)

            if not any(math.isnan(v) for v in [mc1, mc2, rsi1, maxr]):
                # Bearish divergence: price higher high but RSI lower high
                if mc1 > mc2 and rsi1 < maxr and rsi_val <= rsi1:
                    divbear[i] = True

                # Bullish divergence: price lower low but RSI higher low
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

# ─────────────────────────────────────────────────────────────────────
#  BREAK-CONFIRMATION TRIGGER TRACKER
#  Mirrors the Pine `var float bull_trigger / bear_trigger` logic
# ─────────────────────────────────────────────────────────────────────

class TriggerTracker:
    """
    Stateful tracker that persists across polling cycles.
    Mirrors Pine 'var float bull_trigger / bear_trigger'.
    """
    def __init__(self):
        self.bull_trigger: float | None = None  # high of bullish divergence candle
        self.bear_trigger: float | None = None  # low  of bearish divergence candle

    def update(self, row: pd.Series) -> dict:
        """
        Process one closed candle row.
        Returns a dict with keys:
          divbear  → new bearish divergence detected
          divbull  → new bullish divergence detected
          buy      → BUY confirmed (breakout + RSI filter)
          sell     → SELL confirmed (breakout + RSI filter)
        """
        signals = {"divbear": False, "divbull": False, "buy": False, "sell": False}

        rsi_val  = row["rsi"]
        high_val = row["high"]
        low_val  = row["low"]

        # ── Step 1: new divergence → set trigger, reset opposite ──
        if row["divbear_close"]:
            signals["divbear"]    = True
            self.bear_trigger     = low_val   # low of divergence candle
            self.bull_trigger     = None      # reset opposite

        elif row["divbull_close"]:
            signals["divbull"]    = True
            self.bull_trigger     = high_val  # high of divergence candle
            self.bear_trigger     = None      # reset opposite

        # ── Step 2: check breakout ──
        bull_broke = (self.bull_trigger is not None) and (high_val >= self.bull_trigger)
        bear_broke = (self.bear_trigger is not None) and (low_val  <= self.bear_trigger)

        # ── Step 3: confirm only if RSI filter passes ──
        if bull_broke and rsi_val > RSI_BUY_MIN:
            signals["buy"] = True
        if bear_broke and rsi_val < RSI_SELL_MAX:
            signals["sell"] = True

        # ── Step 4: reset trigger on first breakout (no second chances) ──
        if bull_broke:
            self.bull_trigger = None
        if bear_broke:
            self.bear_trigger = None

        return signals

# ─────────────────────────────────────────────────────────────────────
#  ALERT MESSAGE BUILDER
# ─────────────────────────────────────────────────────────────────────

def build_alert(signal_type: str, row: pd.Series) -> str:
    ts  = datetime.fromtimestamp(row["time"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    rsi = f"{row['rsi']:.2f}"
    px  = f"{row['close']:.2f}"

    templates = {
        "divbear": (
            "🔴 <b>Bearish RSI Divergence Detected</b>\n"
            f"Symbol : {SYMBOL}  |  TF : {RESOLUTION}\n"
            f"Time   : {ts}\n"
            f"Close  : {px}  |  RSI : {rsi}\n"
            "⚠️ Watch for SELL break below divergence low"
        ),
        "divbull": (
            "🟢 <b>Bullish RSI Divergence Detected</b>\n"
            f"Symbol : {SYMBOL}  |  TF : {RESOLUTION}\n"
            f"Time   : {ts}\n"
            f"Close  : {px}  |  RSI : {rsi}\n"
            "⚠️ Watch for BUY break above divergence high"
        ),
        "buy": (
            "✅ <b>BUY CONFIRMED ▲</b>\n"
            f"Symbol : {SYMBOL}  |  TF : {RESOLUTION}\n"
            f"Time   : {ts}\n"
            f"Close  : {px}  |  RSI : {rsi}\n"
            "Candle broke above divergence high with RSI > 40"
        ),
        "sell": (
            "🔻 <b>SELL CONFIRMED ▼</b>\n"
            f"Symbol : {SYMBOL}  |  TF : {RESOLUTION}\n"
            f"Time   : {ts}\n"
            f"Close  : {px}  |  RSI : {rsi}\n"
            "Candle broke below divergence low with RSI < 60"
        ),
    }
    return templates.get(signal_type, f"Signal: {signal_type}")

# ─────────────────────────────────────────────────────────────────────
#  CANDLE-CLOSE SYNC HELPER
# ─────────────────────────────────────────────────────────────────────

def seconds_until_next_candle(interval_seconds: int = 300) -> float:
    """
    Returns how many seconds remain until the next candle-close boundary.
    E.g. for 5m candles (300s): if it is 13:57:20, the next boundary is
    14:00:00, so this returns 160.0 seconds.
    """
    now = time.time()
    elapsed_in_current = now % interval_seconds
    return interval_seconds - elapsed_in_current

# ─────────────────────────────────────────────────────────────────────
#  MAIN LOOP
# ─────────────────────────────────────────────────────────────────────

def main():
    log.info("═" * 60)
    log.info("  RSI Divergence Bot  —  Delta Exchange India")
    log.info(f"  Symbol: {SYMBOL}  |  TF: {RESOLUTION}  |  DRY_RUN: {DRY_RUN}")
    log.info("═" * 60)

    if TELEGRAM_TOKEN == "YOUR_BOT_TOKEN_HERE" and not DRY_RUN:
        log.error("Set TELEGRAM_TOKEN in your .env file or enable DRY_RUN=True")
        return

    tracker       = TriggerTracker()
    last_bar_time = None          # track last candle we processed — avoid duplicate alerts

    # ── Sync to next candle-close boundary on startup ─────────────────
    wait = seconds_until_next_candle(POLL_SECONDS)
    next_close = datetime.fromtimestamp(
        time.time() + wait, tz=timezone.utc
    ).strftime("%H:%M:%S UTC")
    log.info(f"Syncing to next candle close in {wait:.0f}s  (at {next_close})")
    time.sleep(wait)



    while True:
        try:
            # ── 1. Fetch candles ──────────────────────────────────────
            df = fetch_candles(SYMBOL, RESOLUTION, CANDLES_FETCH)

            if df.empty or len(df) < RSI_LENGTH + LOOKBACK + 5:
                log.warning("Not enough candles — skipping cycle")
                time.sleep(POLL_SECONDS)
                continue

            # ── 2. RSI ───────────────────────────────────────────────
            df = add_rsi(df, RSI_LENGTH)

            # ── 3. Divergence detection ──────────────────────────────
            df = compute_divergences(df, LOOKBACK)

            # ── 4. Process only the LATEST closed candle ─────────────
            latest = df.iloc[-1]
            bar_time = latest["time"]

            if bar_time == last_bar_time:
                log.info(f"Bar {bar_time} already processed — waiting for next candle")
                time.sleep(POLL_SECONDS)
                continue

            last_bar_time = bar_time
            ts_str = datetime.fromtimestamp(bar_time, tz=timezone.utc).strftime("%H:%M UTC")
            log.info(
                f"Bar {ts_str}  close={latest['close']:.2f}  "
                f"rsi={latest['rsi']:.2f}  "
                f"divbear={latest['divbear_close']}  divbull={latest['divbull_close']}"
            )

            # ── 5. Update trigger tracker + fire alerts ───────────────
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
            log.exception(f"Unexpected error: {exc}")

        # ── 6. Wait exactly one candle interval (already synced) ──────
        # Add a tiny buffer (2s) to ensure the candle is fully closed
        # before the API registers it.
        sleep_secs = POLL_SECONDS + 2
        log.info(f"Sleeping {sleep_secs}s until next candle close…\n")
        time.sleep(sleep_secs)


if __name__ == "__main__":
    main()
