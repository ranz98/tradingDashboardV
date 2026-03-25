"""
cruzebot.py — CruzeBot All-in-One
───────────────────────────────────────────────────────────────────────────────
Thread 1 — Telegram signal listener + trade executor
Thread 2 — Stats pusher (Binance → Vercel KV every 30s)

Signal flow:
  Telegram message → parse_signal() → verify_signal() → notify()
  → execute_trade() → notify result

Usage:
  pip install requests telethon python-dotenv
  python cruzebot.py
"""

import os, time, hmac, hashlib, requests, re, json, asyncio, logging, threading, functions, math
from datetime import datetime, timezone, timedelta

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from telethon import TelegramClient, events

# ═══════════════════════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(name)-7s]  %(message)s",
    datefmt="%H:%M:%S",
)
bot_log  = logging.getLogger("BOT")
push_log = logging.getLogger("PUSHER")

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

# ── Telegram listener (your signal source)
TG_API_ID   = int(os.environ.get("TG_API_ID",  "15382179"))
TG_API_HASH = os.environ.get("TG_API_HASH",    "90a79d047aec413ac623ae7384e264dc")
TG_CHANNEL  = int(os.environ.get("TG_CHANNEL", "-1001947041400"))
TG_SESSION  = os.environ.get("TG_SESSION",     "trading_session")

# ── Telegram notification bot (your alerts channel)
NOTIFY_BOT_TOKEN = os.environ.get("NOTIFY_BOT_TOKEN", "5673530195:AAFgtDbGctsZ4tjdbIh8JxrOFF4h2aneCTg")
NOTIFY_CHAT_ID   = os.environ.get("NOTIFY_CHAT_ID",   "-1001862164693")

# ── Binance
BINANCE_KEY    = os.environ.get("BINANCE_API_KEY",
    "Rm3MMRtjH49D1rqtRQaLkk1G0AaXR1RMgKFe0KOdS6iCxeG9Tt4n1pH8F1F3uQtp")
BINANCE_SECRET = os.environ.get("BINANCE_API_SECRET",
    "9uu4HK9cAmPqnmppnprpfNt1EBBNymbZSKBsRHPoJyjXHWdB5gVe1oA08ip2FY4u")
BINANCE_URL    = os.environ.get("BINANCE_BASE_URL", "https://demo-fapi.binance.com")

LEVERAGE    = int(os.environ.get("LEVERAGE",     "10"))
USDT_AMOUNT = float(os.environ.get("USDT_AMOUNT", "120"))

# ── Vercel KV
KV_URL   = os.environ.get("KV_REST_API_URL",   "")
KV_TOKEN = os.environ.get("KV_REST_API_TOKEN", "")

# Clean KV_URL (remove trailing slashes)
KV_URL = KV_URL.rstrip("/")

# ── Pusher
PUSH_INTERVAL = int(os.environ.get("PUSH_INTERVAL", "30"))
INCOME_DAYS   = int(os.environ.get("INCOME_DAYS",   "90"))

# ───────────────────────────────────────────────────────────────────────────────
#  DAILY SIGNAL / TRADE COUNTERS
# ═══════════════════════════════════════════════════════════════════════════════

_counter_lock = threading.Lock()
_counter_date = datetime.now(timezone.utc).date()   # last reset date

# Counts reset daily at midnight UTC
_signals_rcvd   = 0   # all messages with any parseable content
_valid_signals  = 0   # signals that pass is_tradeable()
_trades_exec    = 0   # trades actually submitted to Binance
_total_signals  = 0   # ALL TIME (not reset, persisted via KV)
_active_targets = {}  # symbol -> {"sl": float, "tp": float} (persisted via KV)
_managed_trades = {}  # symbol -> {"be_set": bool, "entry": float} (persisted via KV)
_event_logs     = []  # list of strings: "[HH:MM:SS] Message"


def add_event_log(msg: str) -> None:
    """Add a timestamped entry to the in-memory event log (last 100)."""
    global _event_logs
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    _event_logs.insert(0, entry)
    if len(_event_logs) > 100:
        _event_logs = _event_logs[:100]
    push_log.info("📝 Log: %s", msg)


def kv_set_key(key: str, val: any) -> bool:
    """Helper to set a KV key using stringified JSON."""
    if not KV_URL or not KV_TOKEN: return False
    try:
        r = requests.post(f"{KV_URL}", headers={"Authorization": f"Bearer {KV_TOKEN}"},
                         json=["SET", key, json.dumps(val)], timeout=10)
        return r.ok
    except Exception as e:
        push_log.error("KV Set Error (%s): %s", key, e)
        return False

def kv_get_key(key: str) -> any:
    """Helper to get and parse a KV key."""
    if not KV_URL or not KV_TOKEN: return None
    try:
        r = requests.get(f"{KV_URL}/get/{key}",
                        headers={"Authorization": f"Bearer {KV_TOKEN}"}, timeout=10)
        if r.ok:
            payload = r.json()
            if payload and payload.get("result"):
                return json.loads(payload["result"])
    except Exception as e:
        push_log.warning("KV Get Error (%s): %s", key, e)
    return None

def load_managed_trades() -> None:
    global _managed_trades
    data = kv_get_key("managed_trades")
    if isinstance(data, dict):
        _managed_trades = data
        push_log.info("💾 Loaded %d managed trades from KV.", len(_managed_trades))

def save_managed_trades() -> None:
    kv_set_key("managed_trades", _managed_trades)

def load_active_targets() -> None:
    global _active_targets
    data = kv_get_key("active_targets")
    if isinstance(data, dict):
        _active_targets = data
        push_log.info("🎯 Loaded %d active targets from KV.", len(_active_targets))

def save_active_targets() -> None:
    kv_set_key("active_targets", _active_targets)

def load_total_signals() -> None:
    global _total_signals
    # We fetch from 'cruzebot_stats' which is where the pusher saves its last snapshot
    data = kv_get_key("cruzebot_stats")
    if isinstance(data, dict):
        prev = data.get("counters", {}).get("totalSignals", 0)
        if isinstance(prev, int):
            _total_signals = prev
            push_log.info("📊 Loaded all-time signals from KV: %d", _total_signals)

def _check_daily_reset() -> None:
    """Reset counters if the UTC date has rolled over."""
    global _counter_date, _signals_rcvd, _valid_signals, _trades_exec
    today = datetime.now(timezone.utc).date()
    if today != _counter_date:
        _counter_date  = today
        _signals_rcvd  = 0
        _valid_signals = 0
        _trades_exec   = 0

def inc_trades_exec() -> None:
    global _trades_exec
    with _counter_lock:
        _check_daily_reset()
        _trades_exec += 1

def inc_signals_rcvd() -> None:
    global _signals_rcvd, _total_signals
    with _counter_lock:
        _check_daily_reset()
        _signals_rcvd += 1
        _total_signals += 1

def inc_valid_signals() -> None:
    global _valid_signals
    with _counter_lock:
        _check_daily_reset()
        _valid_signals += 1


def get_counters() -> dict:
    with _counter_lock:
        _check_daily_reset()
        return {
            "date":          _counter_date.isoformat(),
            "signalsToday":  _signals_rcvd,
            "validSignals":  _valid_signals,
            "tradesExecuted": _trades_exec,
            "totalSignals":  _total_signals,
        }

# ═══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM NOTIFICATIONS
# ═══════════════════════════════════════════════════════════════════════════════

def notify(message: str, parse_mode: str = "HTML") -> None:
    """
    Send a message to your notification Telegram channel.
    Supports HTML formatting (bold, italic, code, etc.).
    Non-blocking — failures are logged only.
    """
    if not NOTIFY_BOT_TOKEN or not NOTIFY_CHAT_ID:
        bot_log.warning("Notify: bot token or chat_id not configured")
        return
    try:
        url = f"https://api.telegram.org/bot{NOTIFY_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": NOTIFY_CHAT_ID,
            "text": message,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        resp = requests.post(url, json=payload, timeout=8)
        data = resp.json()
        if not data.get("ok"):
            # Fallback: retry without parse_mode in case of HTML syntax errors
            bot_log.warning("Notify failed (HTML), retrying plain: %s", data.get("description"))
            plain_payload = {"chat_id": NOTIFY_CHAT_ID, "text": message, "disable_web_page_preview": True}
            resp2 = requests.post(url, json=plain_payload, timeout=8)
            if not resp2.json().get("ok"):
                bot_log.warning("Notify plain also failed: %s", resp2.text)
    except Exception as e:
        bot_log.warning("Notify error: %s", e)


def _esc(text: str) -> str:
    """Escape HTML special characters for Telegram HTML parse mode."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def notify_signal(sig: dict) -> None:
    """
    Send a rich, structured Telegram notification for a parsed signal.
    Covers all verification scenarios:
      - Symbol found / not found
      - Entry found / not found
      - SL found / not found
      - TP found / not found
      - Sanity-check warnings (SL above entry for LONG, etc.)
    """
    sym   = sig["symbol"]
    side  = sig["side"]
    entry = sig["entry"]
    el    = sig["entry_low"]
    eh    = sig["entry_high"]
    sl    = sig["sl"]
    tps   = sig["tps"]
    tp    = sig["tp_target"]
    lev   = sig["leverage"]
    errs  = sig["errors"]

    # ── Verification status icons ─────────────────────────────────────────
    def chk(val): return "✅" if val else "❌"

    sym_ok  = bool(sym)
    side_ok = bool(side)
    sl_ok   = bool(sl)
    tp_ok   = bool(tp)
    entry_ok = bool(entry)

    # ── Build entry string ─────────────────────────────────────────────────
    if el and eh:
        entry_str = f"<code>${_esc(el)} – ${_esc(eh)}</code> <i>(mid: ${_esc(round(entry, 6))})</i>"
    elif entry:
        entry_str = f"<code>${_esc(entry)}</code>"
    else:
        entry_str = "<b>❌ NOT FOUND</b> — will try market price"

    # ── Build TP string ────────────────────────────────────────────────────
    if tps:
        tps_str = "  ".join(f"<code>${_esc(t)}</code>" for t in tps)
    else:
        tps_str = "<b>❌ NOT FOUND</b>"

    # ── Build SL string ───────────────────────────────────────────────────
    sl_str = f"<code>${_esc(sl)}</code>" if sl else "<b>❌ NOT FOUND</b>"

    # ── Side badge ────────────────────────────────────────────────────────
    if side == "LONG":
        side_badge = "🟢 <b>LONG / BUY</b>"
    elif side == "SHORT":
        side_badge = "🔴 <b>SHORT / SELL</b>"
    else:
        side_badge = "<b>❌ NOT FOUND</b>"

    # ── Tradeable? ────────────────────────────────────────────────────────
    tradeable = is_tradeable(sig)
    trade_line = "🚀 <b>EXECUTING TRADE</b>" if tradeable else "🚫 <b>TRADE SKIPPED — missing critical fields</b>"

    # ── Verification summary ──────────────────────────────────────────────
    verify_lines = (
        f"{chk(sym_ok)} Symbol:     <b>{_esc(sym) if sym else 'NOT FOUND'}</b>\n"
        f"{chk(side_ok)} Side:       {side_badge}\n"
        f"{chk(entry_ok)} Entry:      {entry_str}\n"
        f"{chk(sl_ok)} Stop Loss:  {sl_str}\n"
        f"{chk(tp_ok)} Take Profit:{tps_str}\n"
        f"📊 Leverage:    <code>{lev}×</code>"
    )

    # ── Error callouts ────────────────────────────────────────────────────
    err_block = ""
    if errs:
        err_lines = "\n".join(f"  {_esc(e)}" for e in errs)
        err_block = f"\n\n⚠️ <b>ISSUES DETECTED:</b>\n{err_lines}"

    msg = (
        f"📡 <b>SIGNAL RECEIVED</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"{verify_lines}"
        f"{err_block}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"{trade_line}"
    )

    notify(msg)


# ═══════════════════════════════════════════════════════════════════════════════
#  SIGNAL PARSING
# ═══════════════════════════════════════════════════════════════════════════════

def parse_signal(text: str) -> dict:
    """
    Parse a Telegram signal message. Returns a dict with all extracted fields
    and a list of any warnings/errors encountered.

    Handles formats like:
      Sell/Short- RIVER/USDT (Highly Risk)
        Entry Point 18.100- 18.300(CROSS)
        Leverage -10x to 20x
        Targets Tp 1 23.084 Tp 2 23.083 ...
        Stop Loss - 21.789

      EMASTER VIP SIGNAL SELL/Short-SQD/USDT
        Entry Point 0.07850 - 0.08400(CROSS)
        Leverage - 5x to 20x
        Tp 1 0.07700 ... Stop Loss- 0.08500
    """
    t    = text.strip()
    errs = []

    # ── 1. SIDE ──────────────────────────────────────────────────────────────
    side = None
    # Check for explicit LONG/BUY or SHORT/SELL indicators
    if re.search(r"\b(buy|long)\b", t, re.I):
        side = "LONG"
    elif re.search(r"\b(sell|short)\b", t, re.I):
        side = "SHORT"
    # Also handle "Buy/Long" or "Sell/Short" combos
    if re.search(r"buy\s*/\s*long|long\s*/\s*buy", t, re.I):
        side = "LONG"
    elif re.search(r"sell\s*/\s*short|short\s*/\s*sell", t, re.I):
        side = "SHORT"

    if not side:
        errs.append("⚠️ Side (LONG/SHORT) not found")

    # ── 2. SYMBOL ────────────────────────────────────────────────────────────
    symbol = None
    # Try "ABC/USDT" format first (e.g. RIVER/USDT, SQD/USDT)
    m = re.search(r"([A-Z0-9]{2,12})\s*/\s*USDT(?:\.P)?", t, re.I)
    if m:
        symbol = m.group(1).upper() + "USDT"
    else:
        # Fallback: "ABCUSDT" or "ABC/USDT.P"
        m2 = re.search(r"([A-Z0-9]{2,12})USDT(?:\.P)?", t, re.I)
        if m2:
            symbol = m2.group(1).upper() + "USDT"

    if not symbol:
        errs.append("❌ Symbol not found — cannot open trade")

    # ── 3. ENTRY POINT ───────────────────────────────────────────────────────
    entry = None
    entry_low = entry_high = None
    # Match range: "18.100- 18.300" or "0.07850 - 0.08400"
    m = re.search(r"[Ee]ntry\s*[Pp]oint[:\s-]*\s*([\d\.]+)\s*[-–]\s*([\d\.]+)", t)
    if m:
        entry_low  = float(m.group(1))
        entry_high = float(m.group(2))
        entry = round((entry_low + entry_high) / 2, 8)  # use midpoint
    else:
        # Single entry value
        m2 = re.search(r"[Ee]ntry\s*[Pp]oint[:\s-]*\s*([\d\.]+)", t)
        if m2:
            entry = float(m2.group(1))

    if not entry:
        errs.append("⚠️ Entry point not found — will use market price")

    # ── 4. LEVERAGE ──────────────────────────────────────────────────────────
    leverage = LEVERAGE  # default from config
    # Match "10x to 20x" or "5x to 20x" — use lower value
    m = re.search(r"[Ll]everage\s*[-:]*\s*(\d+)[xX]\s*to\s*(\d+)[xX]", t)
    if m:
        leverage = int(m.group(1))  # use lower (safer)
    else:
        # Single leverage: "10x" or "Leverage: 10"
        m2 = re.search(r"[Ll]everage\s*[-:]*\s*(\d+)[xX]?", t)
        if m2:
            leverage = int(m2.group(1))

    # ── 5. TAKE PROFITS ──────────────────────────────────────────────────────
    # Handles all formats:
    #   "Tp 1 23.084"        (plain)
    #   "Tp 1 👉 23.084"     (emoji arrow, common in signal channels)
    #   "Tp 2 👉23.083"      (no space after emoji)
    #   "Tp1 1.005"          (no space before number)
    #   "Tp 1 : 23.084"      (colon separator)
    #   "Tp 3 👉0.0828Tp"    (immediately followed by next TP — no newline)
    #
    # Strategy: after "Tp N", skip any non-digit characters (emojis, arrows,
    # spaces, colons, dashes) then capture the first decimal number.
    tp_matches = re.findall(
        r"[Tt][Pp]\s*\d+\s*[^\d\n]{0,10}?([\d]+(?:\.[\d]+)?)", t
    )
    tps = [float(x) for x in tp_matches if float(x) > 0]

    if not tps:
        errs.append("❌ Take Profit targets not found — cannot open trade")

    # Always use TP5 (last TP) as the target — safest exit with most room
    tp_target = tps[-1] if tps else None

    # ── 6. STOP LOSS ─────────────────────────────────────────────────────────
    sl = None
    # Match "Stop Loss - 21.789", "Stop Loss: 21.789", "SL: 21.789", "Stop Loss- 0.08500"
    m = re.search(r"[Ss]top\s*[Ll]oss\s*[-:–]*\s*([\d\.]+)", t)
    if m:
        sl = float(m.group(1))
    else:
        m2 = re.search(r"\bSL\s*[-:]*\s*([\d\.]+)", t)
        if m2:
            sl = float(m2.group(1))

    if not sl:
        errs.append("❌ Stop Loss not found — cannot open trade")

    # ── 7. SANITY CHECKS ─────────────────────────────────────────────────────
    if sl and entry and side:
        if side == "LONG" and sl >= entry:
            errs.append(f"⚠️ SL ({sl}) is above entry ({entry}) for LONG — check signal")
        if side == "SHORT" and sl <= entry:
            errs.append(f"⚠️ SL ({sl}) is below entry ({entry}) for SHORT — check signal")

    if tps and entry and side:
        if side == "LONG" and tp_target and tp_target <= entry:
            errs.append(f"⚠️ TP ({tp_target}) is below entry ({entry}) for LONG — check signal")
        if side == "SHORT" and tp_target and tp_target >= entry:
            errs.append(f"⚠️ TP ({tp_target}) is above entry ({entry}) for SHORT — check signal")

    return {
        "symbol":     symbol,
        "side":       side,
        "entry":      entry,
        "entry_low":  entry_low,
        "entry_high": entry_high,
        "leverage":   leverage,
        "tps":        tps,
        "tp_target":  tp_target,
        "sl":         sl,
        "errors":     errs,
        "raw":        t[:300],
    }


def is_tradeable(sig: dict) -> bool:
    """Returns True only if all critical fields are present and no hard errors."""
    critical = ["❌"]
    for e in sig["errors"]:
        if any(c in e for c in critical):
            return False
    return bool(sig["symbol"] and sig["side"] and sig["sl"] and sig["tp_target"])


# ═══════════════════════════════════════════════════════════════════════════════
#  BINANCE SHARED HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

_rl_lock           = threading.Lock()
_minute_weight     = 0
_weight_window_end = time.time() + 60.0
WEIGHT_WARN  = 1800
WEIGHT_LIMIT = 2200


def _check_weight(used: int) -> None:
    global _minute_weight, _weight_window_end
    with _rl_lock:
        now = time.time()
        if now >= _weight_window_end:
            _minute_weight     = 0
            _weight_window_end = now + 60.0
        _minute_weight += used
        if _minute_weight >= WEIGHT_LIMIT:
            wait = _weight_window_end - now + 1.0
            push_log.warning("⚠  Rate-limit ceiling — sleeping %.1fs", wait)
            time.sleep(max(wait, 1.0))
            _minute_weight     = 0
            _weight_window_end = time.time() + 60.0
        elif _minute_weight >= WEIGHT_WARN:
            push_log.warning("⚠  Rate-limit at %d/2400 — slowing", _minute_weight)
            time.sleep(2.0)


def _sign(params: dict) -> str:
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    return hmac.new(BINANCE_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()


def binance_request(method: str, endpoint: str, params: dict, weight: int = 5) -> dict:
    _check_weight(weight)
    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 10000 # Increased window for stability
    params["signature"] = _sign(params)
    headers = {"X-MBX-APIKEY": BINANCE_KEY}
    try:
        r = requests.request(method, BINANCE_URL + endpoint,
                             params=params, headers=headers, timeout=10)
        
        if r.status_code >= 400:
            push_log.error("Binance API Error (%d): %s", r.status_code, r.text)

        sw = r.headers.get("X-MBX-USED-WEIGHT-1M")
        if sw:
            with _rl_lock:
                global _minute_weight
                _minute_weight = int(sw)
        return r.json()
    except Exception as e:
        return {"error": str(e)}


def binance_get(endpoint: str, params: dict = None, weight: int = 5):
    p = dict(params or {})
    p["timestamp"] = int(time.time() * 1000)
    p["recvWindow"] = 10000
    p["signature"] = _sign(p)
    _check_weight(weight)
    
    r = requests.get(BINANCE_URL + endpoint, params=p,
                     headers={"X-MBX-APIKEY": BINANCE_KEY}, timeout=15)
    
    if r.status_code >= 400:
        push_log.error("Binance GET Error (%d): %s", r.status_code, r.text)
        r.raise_for_status()

    sw = r.headers.get("X-MBX-USED-WEIGHT-1M")
    if sw:
        with _rl_lock:
            global _minute_weight
            _minute_weight = int(sw)
    r.raise_for_status()
    return r.json()


def get_price(symbol: str) -> float | None:
    try:
        r = requests.get(f"{BINANCE_URL}/fapi/v1/ticker/price",
                         params={"symbol": symbol}, timeout=5).json()
        return float(r["price"])
    except:
        return None


def get_precision_data(symbol: str) -> tuple[int, int]:
    """Returns (pricePrecision, quantityPrecision) for a symbol."""
    try:
        info = requests.get(f"{BINANCE_URL}/fapi/v1/exchangeInfo", timeout=8).json()
        for s in info.get("symbols", []):
            if s["symbol"] == symbol:
                tick_size = float(next(f['tickSize'] for f in s['filters'] if f['filterType'] == 'PRICE_FILTER'))
                price_prec = max(0, -int(round(math.log10(tick_size))))
                qty_prec = int(s["quantityPrecision"])
                return price_prec, qty_prec
    except: pass
    return 8, 3 # Default fallback

def get_qty(symbol: str, usdt_amount: float = USDT_AMOUNT, lev: int = LEVERAGE) -> float:
    price = get_price(symbol)
    if not price: return 0
    try:
        _, qty_prec = get_precision_data(symbol)
        return round(usdt_amount / price, qty_prec)
    except:
        return round(usdt_amount / price, 1)


# ═══════════════════════════════════════════════════════════════════════════════
#  TRADE EXECUTOR
# ═══════════════════════════════════════════════════════════════════════════════

async def execute_trade(sig: dict, semaphore: asyncio.Semaphore) -> None:
    symbol = sig["symbol"]
    side   = sig["side"]
    sl     = sig["sl"]
    tp     = sig["tp_target"]
    lev    = sig.get("leverage", LEVERAGE)

    async with semaphore:
        try:
            bot_log.info("⏳  Executing %s %s  SL=%.6f  TP=%.6f  Lev=%dx",
                         symbol, side, sl, tp, lev)

            # 1. Set leverage
            binance_request("POST", "/fapi/v1/leverage",
                            {"symbol": symbol, "leverage": lev})
            await asyncio.sleep(0.2)

            # 2. Quantity
            qty = get_qty(symbol, USDT_AMOUNT, lev)
            if qty <= 0:
                msg = f"❌ TRADE FAILED\n{symbol}: Could not calculate quantity — price unavailable"
                bot_log.error(msg)
                notify(msg)
                return

            open_side  = "BUY"  if side == "LONG" else "SELL"
            close_side = "SELL" if side == "LONG" else "BUY"

            # 3. Market open
            resp = binance_request("POST", "/fapi/v1/order", {
                "symbol": symbol, "side": open_side,
                "type": "MARKET", "quantity": qty,
            })

            if "orderId" not in resp:
                err = resp.get("msg", str(resp))
                msg = f"❌ TRADE FAILED\n{symbol} {side}\nBinance error: {err}"
                bot_log.error(msg)
                notify(msg)
                return

            price_filled = float(resp.get("avgPrice", 0) or get_price(symbol) or 0)
            bot_log.info("✅  %s OPENED @ %.6f (qty=%s)", symbol, price_filled, qty)
            await asyncio.sleep(0.2)
            inc_trades_exec()  # count successful trade submissions

            # 4. Round SL/TP
            price_prec, _ = get_precision_data(symbol)
            sl_p = round(float(sl), int(price_prec))
            tp_p = round(float(tp), int(price_prec))

            # 5. Stop-loss
            sl_resp = binance_request("POST", "/fapi/v1/order", {
                "symbol": symbol, "side": close_side,
                "type": "STOP_MARKET", "stopPrice": str(sl_p), "closePosition": "true",
            })
            if "orderId" not in sl_resp:
                bot_log.info("⚠️  Standard SL rejected (code %s), trying Algo fallback...", sl_resp.get('code'))
                sl_resp = binance_request("POST", "/fapi/v1/algoOrder", {
                    "algoType": "CONDITIONAL", "symbol": symbol, "side": close_side,
                    "type": "STOP_MARKET", "triggerPrice": str(sl_p),
                    "closePosition": "true", "workingType": "MARK_PRICE", "timeInForce": "GTC"
                })

            # 6. Take-profit
            tp_resp = binance_request("POST", "/fapi/v1/order", {
                "symbol": symbol, "side": close_side,
                "type": "TAKE_PROFIT_MARKET", "stopPrice": str(tp_p), "closePosition": "true",
            })
            if "orderId" not in tp_resp:
                bot_log.info("⚠️  Standard TP rejected (code %s), trying Algo fallback...", tp_resp.get('code'))
                tp_resp = binance_request("POST", "/fapi/v1/algoOrder", {
                    "algoType": "CONDITIONAL", "symbol": symbol, "side": close_side,
                    "type": "TAKE_PROFIT_MARKET", "triggerPrice": str(tp_p),
                    "closePosition": "true", "workingType": "MARK_PRICE", "timeInForce": "GTC"
                })

            sl_ok = ("orderId" in sl_resp or "algoId" in sl_resp)
            tp_ok = ("orderId" in tp_resp or "algoId" in tp_resp)

            sl_warn = "" if sl_ok else f"\n⚠️ <b>SL ORDER FAILED</b> — {sl_resp.get('msg','unknown')}\n🔴 <b>SET MANUAL SL AT <code>${sl_p:.6f}</code> IMMEDIATELY</b>"
            tp_warn = "" if tp_ok else f"\n⚠️ <b>TP ORDER FAILED</b> — {tp_resp.get('msg','unknown')}"

            _active_targets[symbol] = {"sl": sl, "tp": tp}
            save_active_targets()

            notify(
                f"✅ <b>TRADE OPENED</b>\n"
                f"━━━━━━━━━━━━━━━\n"
                f"🪙 Pair:  <b>{_esc(symbol)}</b>\n"
                f"{'🟢' if side=='LONG' else '🔴'} Side:  <b>{_esc(side)}</b>\n"
                f"💰 Entry: <code>${price_filled:.6f}</code>\n"
                f"📦 Qty:   <code>{qty}</code>\n"
                f"⚡ Lev:   <code>{lev}×</code>\n"
                f"🛑 SL:    <code>${sl:.6f}</code>  {'✅' if sl_ok else '❌ FAILED'}\n"
                f"🎯 TP:    <code>${tp:.6f}</code>  {'✅' if tp_ok else '❌ FAILED'}\n"
                f"━━━━━━━━━━━━━━━"
                f"{sl_warn}{tp_warn}"
            )

            await asyncio.sleep(0.5)

        except Exception as e:
            msg = f"⚠️ UNEXPECTED ERROR\n{symbol}: {e}"
            bot_log.error(msg)
            notify(msg)


# ═══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM SIGNAL LISTENER
# ═══════════════════════════════════════════════════════════════════════════════

async def run_telegram_bot() -> None:
    semaphore = asyncio.Semaphore(2)
    client    = TelegramClient(TG_SESSION, TG_API_ID, TG_API_HASH)

    @client.on(events.NewMessage(chats=TG_CHANNEL))
    async def on_message(event):
        if not event.message.text:
            return

        text = event.message.text
        bot_log.info("📩  New message received (%d chars)", len(text))

        sig = parse_signal(text)
        add_event_log(f"Signal detected: {sig['symbol']} {sig['side']}")
        inc_signals_rcvd()  # count every parseable signal

        # ── Send rich structured notification (includes verification + errors) ──
        notify_signal(sig)

        # ── Log any warnings locally as well ─────────────────────────────────
        if sig["errors"]:
            for e in sig["errors"]:
                bot_log.warning("Signal issue: %s", e)

        # ── Decide whether to trade ─────────────────────────────────────────
        if not is_tradeable(sig):
            bot_log.warning("Signal not tradeable — skipping")
            return

        inc_valid_signals()  # passed all checks

        # ── Execute ─────────────────────────────────────────────────────────
        add_event_log(f"Executing trade: {sig['symbol']}")
        asyncio.create_task(execute_trade(sig, semaphore))
        await asyncio.sleep(0.5)

    bot_log.info("🚀  Connecting to Telegram …")
    await client.start()
    bot_log.info("✅  Listening on channel %s", TG_CHANNEL)
    await client.run_until_disconnected()


# ═══════════════════════════════════════════════════════════════════════════════
#  STATS PUSHER (Thread 2)
# ═══════════════════════════════════════════════════════════════════════════════

def kv_set(key: str, value: dict) -> bool:
    if not KV_URL or not KV_TOKEN:
        push_log.warning("KV not configured — skipping push")
        return False
    try:
        r = requests.post(KV_URL,
                          headers={"Authorization": f"Bearer {KV_TOKEN}",
                                   "Content-Type": "application/json"},
                          json=["SET", key, json.dumps(value)], timeout=10)
        r.raise_for_status()
        return True
    except Exception as e:
        push_log.error("KV push failed: %s", e)
        return False


def collect_stats() -> dict:
    utc = datetime.now(timezone.utc)
    def ms_ago(d): return int((utc - timedelta(days=d)).timestamp() * 1000)
    def today_ms(): return int(utc.replace(hour=0,minute=0,second=0,microsecond=0).timestamp()*1000)
    def week_ms():
        mon = utc - timedelta(days=utc.weekday())
        return int(mon.replace(hour=0,minute=0,second=0,microsecond=0).timestamp()*1000)

    push_log.info("📡  Collecting Binance data …")
    acct    = binance_get("/fapi/v2/account", weight=5)
    raw_pos = binance_get("/fapi/v2/positionRisk", weight=5)
    raw_inc = binance_get("/fapi/v1/income",
                          {"incomeType": "REALIZED_PNL",
                           "startTime": ms_ago(INCOME_DAYS), "limit": 1000}, weight=20)

    # ── 4. Fetch Live SL/TP from Binance Open Orders ────────────────────────
    try:
        open_std = binance_get("/fapi/v1/openOrders", weight=5)
        # Some symbols use Algo Orders for SL/TP
        open_algo_raw = binance_get("/fapi/v1/algoOrder/openAlgoOrders", weight=5)
        # Check if response is wrapped in an object or just a list
        open_algo = open_algo_raw.get("orders", open_algo_raw) if isinstance(open_algo_raw, dict) else open_algo_raw
    except Exception as e:
        push_log.warning("Could not fetch live orders: %s", e)
        open_std = []; open_algo = []

    live_stops = {}
    for o in (open_std if isinstance(open_std, list) else []):
        s = o["symbol"]
        if s not in live_stops: live_stops[s] = {"sl": None, "tp": None}
        ot = o.get("type", "")
        if ot == "STOP_MARKET": live_stops[s]["sl"] = float(o.get("stopPrice", 0))
        elif ot == "TAKE_PROFIT_MARKET": live_stops[s]["tp"] = float(o.get("stopPrice", 0))

    for o in (open_algo if isinstance(open_algo, list) else []):
        s = o["symbol"]
        if s not in live_stops: live_stops[s] = {"sl": None, "tp": None}
        at = o.get("type", "")
        if at == "STOP_MARKET": live_stops[s]["sl"] = float(o.get("triggerPrice", 0))
        elif at == "TAKE_PROFIT_MARKET": live_stops[s]["tp"] = float(o.get("triggerPrice", 0))

    positions = []
    for p in (raw_pos if isinstance(raw_pos, list) else []):
        amt = float(p.get("positionAmt", 0))
        if amt == 0: continue
        entry = float(p.get("entryPrice", 0)); mark = float(p.get("markPrice", 0))
        upnl  = float(p.get("unRealizedProfit", 0)); liq = float(p.get("liquidationPrice", 0))
        lev   = int(p.get("leverage",1))
        pct   = (((mark-entry)/entry*100*(1 if amt>0 else -1)) * lev) if entry else 0
        
        # Use live data from exchange if available, otherwise fall back to local target state
        ls = live_stops.get(p["symbol"], {})
        tgt = _active_targets.get(p["symbol"], {})
        final_sl = ls.get("sl") or tgt.get("sl")
        final_tp = ls.get("tp") or tgt.get("tp")

        positions.append({"symbol": p["symbol"], "side": "LONG" if amt>0 else "SHORT",
            "size": abs(amt), "entryPrice": round(entry,6), "markPrice": round(mark,6),
            "liqPrice": round(liq,6) if liq else None,
            "unrealizedPnl": round(upnl,4), "changePct": round(pct,3),
            "leverage": lev,
            "sl": final_sl, "tp": final_tp})

    trades = [{"ts": i["time"],
               "time": datetime.utcfromtimestamp(i["time"]/1000).strftime("%Y-%m-%d %H:%M"),
               "symbol": i.get("symbol",""), "pnl": round(float(i.get("income",0)),4)}
              for i in (raw_inc if isinstance(raw_inc,list) else [])]

    t0,tw  = today_ms(), week_ms()
    wins   = [t for t in trades if t["pnl"]>0]
    losses = [t for t in trades if t["pnl"]<0]
    total_pnl = sum(t["pnl"] for t in trades)
    today_pnl = sum(t["pnl"] for t in trades if t["ts"]>=t0)
    week_pnl  = sum(t["pnl"] for t in trades if t["ts"]>=tw)
    win_rate  = len(wins)/len(trades)*100 if trades else 0
    avg_win   = sum(t["pnl"] for t in wins)/len(wins)     if wins   else 0
    avg_loss  = sum(t["pnl"] for t in losses)/len(losses) if losses else 0

    daily = {}
    for t in sorted(trades, key=lambda x: x["ts"]):
        d = t["time"][:10]; daily[d] = round(daily.get(d,0)+t["pnl"],4)
    cum, curve = 0.0, []
    for d,pnl in sorted(daily.items()):
        cum += pnl; curve.append({"date":d,"dailyPnl":round(pnl,4),"cumPnl":round(cum,4)})

    by_sym = {}
    for t in trades:
        s = t["symbol"]
        if s not in by_sym: by_sym[s]={"symbol":s,"trades":0,"pnl":0.0,"wins":0,"losses":0}
        by_sym[s]["trades"]+=1; by_sym[s]["pnl"]=round(by_sym[s]["pnl"]+t["pnl"],4)
        if t["pnl"]>0: by_sym[s]["wins"]+=1
        elif t["pnl"]<0: by_sym[s]["losses"]+=1

    streak = {"type":"none","count":0}
    if trades:
        ordered = sorted(trades, key=lambda t: t["ts"])
        st = "win" if ordered[-1]["pnl"]>0 else "loss"; cnt=0
        for t in reversed(ordered):
            if (st=="win" and t["pnl"]>0) or (st=="loss" and t["pnl"]<0): cnt+=1
            else: break
        streak = {"type":st,"count":cnt}

    return {
        "pushedAt": utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "counters": get_counters(),
        "account": {
            "walletBalance":    round(float(acct.get("totalWalletBalance",    0)),2),
            "availableBalance": round(float(acct.get("availableBalance",      0)),2),
            "unrealizedProfit": round(float(acct.get("totalUnrealizedProfit", 0)),4),
            "initialMargin":    round(float(acct.get("totalInitialMargin",    0)),2),
            "marginBalance":    round(float(acct.get("totalMarginBalance",    0)),2),
            "maintMargin":      round(float(acct.get("totalMaintMargin",      0)),2),
        },
        "positions": positions,
        "stats": {
            "totalPnl":    round(total_pnl,2), "todayPnl": round(today_pnl,2),
            "weekPnl":     round(week_pnl,2),  "winRate":  round(win_rate,2),
            "avgWin":      round(avg_win,2),    "avgLoss":  round(avg_loss,2),
            "bestTrade":   max(trades, key=lambda t: t["pnl"], default=None),
            "worstTrade":  min(trades, key=lambda t: t["pnl"], default=None),
            "totalTrades": len(trades), "wins": len(wins), "losses": len(losses),
            "streak":      streak,
            "bySymbol":    sorted(by_sym.values(), key=lambda x: x["pnl"], reverse=True),
            "curve":       curve,
            # Full trade history (no cap) — dashboard does client-side date filtering
            "allTrades":    sorted(trades, key=lambda x: x["ts"], reverse=True),
            "recentTrades": sorted(trades, key=lambda x: x["ts"], reverse=True)[:200],
        },
        "logs":      _event_logs,
    }

def run_stats_pusher() -> None:
    push_log.info("🚀  Stats pusher started (interval=%ds)", PUSH_INTERVAL)
    if not KV_URL:
        push_log.warning("KV_REST_API_URL not set — stats will not push to Vercel")
    while True:
        start = time.time()
        try:
            snap = collect_stats()
            
            # --- TRADE MANAGEMENT logic via functions.py ---
            current_syms = [p["symbol"] for p in snap["positions"]]
            
            # 1. Manage existing trades (50% profit / Move to BE)
            # Pass inc_trades_exec so partial closes are counted
            changed_mgr = functions.manage_open_trades(
                snap["positions"], _managed_trades, binance_request, notify, inc_trades_exec, add_event_log
            )
            if changed_mgr:
                save_managed_trades()
            
            # 2. Detect closures (TP/SL Hit)
            changed_cls = functions.check_trade_closures(
                current_syms, _active_targets, binance_request, notify, add_event_log
            )
            if changed_cls:
                save_active_targets()

            # 3. ALWAYS prune managed_trades to match live positions
            # This ensures closed trades are wiped even if they closed while bot was off
            original_mgr_len = len(_managed_trades)
            for s in list(_managed_trades.keys()):
                if s not in current_syms:
                    del _managed_trades[s]
            
            if changed_cls or len(_managed_trades) != original_mgr_len:
                save_managed_trades()
            # ---------------------------------------------

            ok = kv_set_key("cruzebot_stats", snap)
            if ok:
                push_log.info("✅  Pushed → PNL %+.2f | Today %+.2f | Pos %d | WR %.1f%%",
                              snap["stats"]["totalPnl"], snap["stats"]["todayPnl"],
                              len(snap["positions"]),    snap["stats"]["winRate"])
        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if e.response else 0
            if code == 429:
                push_log.error("🚫 429 — sleeping 60s");  time.sleep(60)
            elif code == 418:
                push_log.error("🚫 418 banned — sleeping 5 min"); time.sleep(300)
            else:
                push_log.error("HTTP error: %s", e)
        except Exception as e:
            push_log.error("Error: %s", e)

        time.sleep(max(0, PUSH_INTERVAL - (time.time() - start)))


# ═══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("""
  ╔══════════════════════════════════════════╗
  ║   🤖  CruzeBot  —  All-in-One            ║
  ║   Thread 1 › Telegram listener + trades  ║
  ║   Thread 2 › Vercel stats pusher         ║
  ╚══════════════════════════════════════════╝
""")
    # Load persistent state
    load_total_signals()
    load_active_targets()
    load_managed_trades()

    # Thread 2: stats pusher — daemon so it dies when main exits
    threading.Thread(target=run_stats_pusher, name="StatsPusher", daemon=True).start()

    # Thread 1: Telegram bot on main thread (telethon needs a stable event loop)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run_telegram_bot())
    except KeyboardInterrupt:
        print("\n👋  Shutting down …")
    finally:
        loop.close()
