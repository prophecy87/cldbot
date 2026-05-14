"""
EliteForge v36 — Stream Engine
================================
Run this as a standalone process:  python stream_engine.py

- Opens Alpaca WebSocket streams for 1m bars (crypto + equities)
- Maintains a rolling OHLCV buffer per symbol in memory
- Computes scalper (1m) and swing (1h synthetic) signals on every bar close
- Executes orders via Alpaca when signals fire
- Writes all state to SQLite (engine.db) so the dashboard can read it
- Never stops — reconnects automatically on drop

Python 3.12  |  Alpaca SDK  |  pandas_ta
"""

import asyncio
import json
import logging
import math
import os
import sqlite3
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pandas_ta as ta
from alpaca.data.live import CryptoDataStream, StockDataStream
from alpaca.data.historical import CryptoHistoricalDataClient, StockHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

# ── logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("engine")

# ── credentials — loaded from .streamlit/secrets.toml (same file Streamlit uses)
# Never hardcode keys here; secrets.toml is listed in .gitignore
def _load_secrets() -> tuple[str, str]:
    """
    Reads .streamlit/secrets.toml so the engine and dashboard share one
    credentials file. Falls back to environment variables for CI/server use.
    """
    secrets_path = Path(__file__).parent / ".streamlit" / "secrets.toml"
    if secrets_path.exists():
        try:
            import tomllib                          # stdlib in Python 3.11+
        except ImportError:
            import tomli as tomllib                 # pip install tomli  (3.10 and below)
        with open(secrets_path, "rb") as f:
            data = tomllib.load(f)
        alpaca = data.get("alpaca", {})
        key    = alpaca.get("api_key")    or alpaca.get("API_KEY")
        secret = alpaca.get("secret_key") or alpaca.get("SECRET_KEY")
        if key and secret:
            return key, secret
    # Fallback: environment variables
    key    = os.environ.get("ALPACA_API_KEY")
    secret = os.environ.get("ALPACA_SECRET_KEY")
    if key and secret:
        return key, secret
    raise RuntimeError(
        "Alpaca credentials not found.\n"
        "Add them to .streamlit/secrets.toml:\n\n"
        "  [alpaca]\n"
        "  api_key    = \"YOUR_KEY\"\n"
        "  secret_key = \"YOUR_SECRET\"\n"
    )

API_KEY, SECRET_KEY = _load_secrets()
PAPER = True          # set False for live

RISK_PCT      = 2.0        # % of equity risked per trade
MAX_POSITIONS = 5
DRAWDOWN_HALT = 8.0        # halt if equity drops this % from session high

CRYPTO_ONLY   = False      # True = $100 mode (crypto only)

CRYPTO_ASSETS = ["BTC/USD", "ETH/USD", "SOL/USD"]
EQUITY_ASSETS = ["NVDA", "TSLA", "MSTR"]          # ignored when CRYPTO_ONLY

# Bars to keep in rolling buffer (enough for all indicators)
BUFFER_SIZE = 300

DB_PATH = Path(__file__).parent / "engine.db"

# ── SQLite schema ──────────────────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS engine_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS signals (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         TEXT    NOT NULL,
    symbol     TEXT    NOT NULL,
    mode       TEXT    NOT NULL,   -- SCALP | SWING
    signal     TEXT    NOT NULL,   -- BUY | SELL | HOLD
    price      REAL    NOT NULL,
    atr        REAL    NOT NULL,
    detail     TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS trades (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         TEXT    NOT NULL,
    symbol     TEXT    NOT NULL,
    side       TEXT    NOT NULL,
    qty        REAL    NOT NULL,
    price      REAL    NOT NULL,
    reason     TEXT    NOT NULL,
    status     TEXT    NOT NULL    -- OK | ERROR
);

CREATE TABLE IF NOT EXISTS bar_latest (
    symbol  TEXT PRIMARY KEY,
    ts      TEXT,
    open    REAL, high REAL, low REAL, close REAL, volume REAL
);
"""

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    return conn

def db_set(conn: sqlite3.Connection, key: str, value) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO engine_state (key, value) VALUES (?, ?)",
        (key, json.dumps(value)),
    )
    conn.commit()

def db_get(conn: sqlite3.Connection, key: str, default=None):
    row = conn.execute(
        "SELECT value FROM engine_state WHERE key = ?", (key,)
    ).fetchone()
    return json.loads(row["value"]) if row else default

# ── Alpaca clients ─────────────────────────────────────────────────────────────
trade_client  = TradingClient(API_KEY, SECRET_KEY, paper=PAPER)
crypto_hist   = CryptoHistoricalDataClient(API_KEY, SECRET_KEY)
stock_hist    = StockHistoricalDataClient(API_KEY, SECRET_KEY)

# ── rolling bar buffers — keyed by symbol ─────────────────────────────────────
# Each entry: deque of dicts {ts, open, high, low, close, volume}
bar_buffers: dict[str, deque] = {}

def ensure_buffer(symbol: str) -> deque:
    if symbol not in bar_buffers:
        bar_buffers[symbol] = deque(maxlen=BUFFER_SIZE)
    return bar_buffers[symbol]

def buffer_to_df(symbol: str) -> pd.DataFrame | None:
    buf = bar_buffers.get(symbol)
    if not buf or len(buf) < 50:
        return None
    df = pd.DataFrame(list(buf))
    df["ts"] = pd.to_datetime(df["ts"])
    df.set_index("ts", inplace=True)
    return df

# ── seed buffers with historical bars so indicators are warm on startup ────────
def seed_buffers() -> None:
    log.info("Seeding bar buffers with historical data…")
    from datetime import timedelta
    start = datetime.now(timezone.utc) - timedelta(days=3)

    for sym in CRYPTO_ASSETS:
        try:
            req  = CryptoBarsRequest(symbol_or_symbols=sym,
                                     timeframe=TimeFrame(1, TimeFrameUnit.Minute),
                                     start=start)
            bars = crypto_hist.get_crypto_bars(req).df
            bars = bars.reset_index()
            buf  = ensure_buffer(sym)
            for _, row in bars.iterrows():
                buf.append({
                    "ts": str(row.get("timestamp", row.name)),
                    "open": float(row["open"]), "high": float(row["high"]),
                    "low":  float(row["low"]),  "close": float(row["close"]),
                    "volume": float(row["volume"]),
                })
            log.info(f"  {sym}: {len(buf)} bars loaded")
        except Exception as e:
            log.warning(f"  Seed failed for {sym}: {e}")

    if not CRYPTO_ONLY:
        for sym in EQUITY_ASSETS:
            try:
                req  = StockBarsRequest(symbol_or_symbols=sym,
                                        timeframe=TimeFrame(1, TimeFrameUnit.Minute),
                                        start=start)
                bars = stock_hist.get_stock_bars(req).df
                bars = bars.reset_index()
                buf  = ensure_buffer(sym)
                for _, row in bars.iterrows():
                    buf.append({
                        "ts": str(row.get("timestamp", row.name)),
                        "open": float(row["open"]), "high": float(row["high"]),
                        "low":  float(row["low"]),  "close": float(row["close"]),
                        "volume": float(row["volume"]),
                    })
                log.info(f"  {sym}: {len(buf)} bars loaded")
            except Exception as e:
                log.warning(f"  Seed failed for {sym}: {e}")

# ── account helpers ────────────────────────────────────────────────────────────
def get_equity() -> float:
    try:
        return float(trade_client.get_account().equity)
    except Exception:
        return 0.0

def get_positions() -> dict[str, float]:
    """Returns {symbol: qty} for all open positions."""
    try:
        return {p.symbol: float(p.qty) for p in trade_client.get_all_positions()}
    except Exception:
        return {}

def open_position_count() -> int:
    return len(get_positions())

# ── position sizing ────────────────────────────────────────────────────────────
def calc_qty(price: float, equity: float, atr: float, is_crypto: bool) -> float:
    if price <= 0 or atr <= 0:
        return 0.0
    risk_dollars  = equity * (RISK_PCT / 100.0)
    stop_distance = max(atr * 1.5, price * 0.005)
    qty           = min(risk_dollars / stop_distance, risk_dollars / price)
    if is_crypto:
        return round(qty, 6)
    qty = math.floor(qty)
    return float(qty) if qty >= 1 else 0.0

# ── order execution ────────────────────────────────────────────────────────────
def place_order(conn: sqlite3.Connection,
                symbol: str, side: OrderSide, qty: float,
                price: float, reason: str) -> bool:
    ts = datetime.now(timezone.utc).isoformat()
    try:
        req = MarketOrderRequest(
            symbol=symbol.replace("/", ""),
            qty=qty,
            side=side,
            time_in_force=TimeInForce.GTC,
        )
        trade_client.submit_order(req)
        log.info(f"ORDER  {side.value.upper():4s}  {symbol}  qty={qty}  @ ~${price:.2f}  [{reason}]")
        conn.execute(
            "INSERT INTO trades (ts, symbol, side, qty, price, reason, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (ts, symbol, side.value.upper(), qty, price, reason, "OK"),
        )
        conn.commit()
        return True
    except Exception as e:
        log.error(f"ORDER ERROR  {symbol}: {e}")
        conn.execute(
            "INSERT INTO trades (ts, symbol, side, qty, price, reason, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (ts, symbol, side.value.upper(), qty, price, str(e), "ERROR"),
        )
        conn.commit()
        return False

# ── circuit breaker ────────────────────────────────────────────────────────────
def check_circuit_breaker(conn: sqlite3.Connection, equity: float) -> bool:
    high = db_get(conn, "session_high_equity", 0.0)
    if equity > high:
        high = equity
        db_set(conn, "session_high_equity", high)
    halted = db_get(conn, "halted", False)
    if not halted and high > 0:
        drawdown = (high - equity) / high * 100
        if drawdown >= DRAWDOWN_HALT:
            log.warning(f"CIRCUIT BREAKER — drawdown {drawdown:.2f}% ≥ {DRAWDOWN_HALT}%")
            db_set(conn, "halted", True)
            halted = True
    return halted

# ── signal engines ─────────────────────────────────────────────────────────────
def scalper_signal(symbol: str) -> dict:
    """
    1m scalper:
      BUY  — EMA9 crosses above EMA21, RSI 35-65, volume > 1.5× 20-bar avg
      SELL — EMA9 crosses below EMA21, OR RSI > 72
    """
    df = buffer_to_df(symbol)
    if df is None:
        return {"signal": "NO DATA", "price": 0.0, "atr": 0.0, "detail": "buffer thin"}

    df["ema9"]  = ta.ema(df["close"], length=9)
    df["ema21"] = ta.ema(df["close"], length=21)
    df["rsi"]   = ta.rsi(df["close"], length=14)
    df["atr"]   = ta.atr(df["high"], df["low"], df["close"], length=14)
    vol_mean    = df["volume"].rolling(20).mean()

    last  = df.iloc[-1]
    prev  = df.iloc[-2]
    price = float(last["close"])
    atr   = float(last["atr"]) if pd.notna(last["atr"]) else price * 0.01
    rsi   = float(last["rsi"]) if pd.notna(last["rsi"]) else 50.0
    vol_surge = float(last["volume"]) > float(vol_mean.iloc[-1]) * 1.5

    ema_up   = float(prev["ema9"]) <= float(prev["ema21"]) and float(last["ema9"]) > float(last["ema21"])
    ema_down = float(prev["ema9"]) >= float(prev["ema21"]) and float(last["ema9"]) < float(last["ema21"])

    if ema_up and 35 < rsi < 65 and vol_surge:
        signal = "BUY"
    elif ema_down or rsi > 72:
        signal = "SELL"
    else:
        signal = "HOLD"

    return {
        "signal": signal, "price": price, "atr": atr,
        "detail": f"EMA9={float(last['ema9']):.2f} EMA21={float(last['ema21']):.2f} RSI={rsi:.1f}",
    }

def swing_signal(symbol: str) -> dict:
    """
    Synthetic 1h swing from 1m buffer (resample last 200×1m bars → 1h):
      BUY  — price > EMA50, MACD histogram flips positive, RSI < 70
      SELL — price < EMA50, OR MACD histogram flips negative, OR RSI > 75
    """
    df = buffer_to_df(symbol)
    if df is None:
        return {"signal": "NO DATA", "price": 0.0, "atr": 0.0, "detail": "buffer thin"}

    # Resample 1m → 1h for swing indicators
    df_h = df.resample("1h").agg({
        "open": "first", "high": "max", "low": "min",
        "close": "last", "volume": "sum",
    }).dropna()

    if len(df_h) < 30:
        return {"signal": "NO DATA", "price": 0.0, "atr": 0.0, "detail": "not enough 1h bars"}

    df_h["ema50"]  = ta.ema(df_h["close"], length=min(50, len(df_h) - 1))
    macd_df        = ta.macd(df_h["close"], fast=12, slow=26, signal=9)
    df_h["macd_h"] = macd_df["MACDh_12_26_9"] if macd_df is not None else 0.0
    df_h["rsi"]    = ta.rsi(df_h["close"], length=14)
    df_h["atr"]    = ta.atr(df_h["high"], df_h["low"], df_h["close"], length=14)

    last  = df_h.iloc[-1]
    prev  = df_h.iloc[-2]
    price = float(df["close"].iloc[-1])   # use 1m price for accuracy
    atr   = float(last["atr"]) if pd.notna(last["atr"]) else price * 0.015
    rsi   = float(last["rsi"]) if pd.notna(last["rsi"]) else 50.0
    ema50 = float(last["ema50"]) if pd.notna(last["ema50"]) else price

    above_ema  = price > ema50
    macd_up    = float(prev["macd_h"]) < 0 and float(last["macd_h"]) > 0
    macd_down  = float(prev["macd_h"]) > 0 and float(last["macd_h"]) < 0

    if above_ema and macd_up and rsi < 70:
        signal = "BUY"
    elif (not above_ema) or macd_down or rsi > 75:
        signal = "SELL"
    else:
        signal = "HOLD"

    return {
        "signal": signal, "price": price, "atr": atr,
        "detail": f"EMA50={ema50:.2f} MACD_H={float(last['macd_h']):.4f} RSI={rsi:.1f}",
    }

# ── core signal processor — called on every bar close ─────────────────────────
def process_bar(conn: sqlite3.Connection, symbol: str, is_crypto: bool) -> None:
    equity = get_equity()
    halted = check_circuit_breaker(conn, equity)
    db_set(conn, "equity",      equity)
    db_set(conn, "last_tick",   datetime.now(timezone.utc).isoformat())
    db_set(conn, "engine_live", True)

    if halted:
        return

    positions = get_positions()
    n_pos     = len(positions)
    sym_clean = symbol.replace("/", "")

    ts = datetime.now(timezone.utc).isoformat()

    # ── scalper signal ─────────────────────────────────────────────────────────
    if not CRYPTO_ONLY or is_crypto:
        sc = scalper_signal(symbol)
        conn.execute(
            "INSERT INTO signals (ts, symbol, mode, signal, price, atr, detail) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (ts, symbol, "SCALP", sc["signal"], sc["price"], sc["atr"], sc["detail"]),
        )

        if sc["signal"] == "BUY" and sym_clean not in positions and n_pos < MAX_POSITIONS:
            qty = calc_qty(sc["price"], equity, sc["atr"], is_crypto)
            if place_order(conn, symbol, OrderSide.BUY, qty, sc["price"],
                           f"SCALP | {sc['detail']}"):
                n_pos += 1

        elif sc["signal"] == "SELL" and sym_clean in positions:
            qty = positions[sym_clean]
            place_order(conn, symbol, OrderSide.SELL, qty, sc["price"],
                        f"SCALP exit | {sc['detail']}")

    # ── swing signal (runs on every bar but acts on 1h resampled data) ─────────
    sw = swing_signal(symbol)
    conn.execute(
        "INSERT INTO signals (ts, symbol, mode, signal, price, atr, detail) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (ts, symbol, "SWING", sw["signal"], sw["price"], sw["atr"], sw["detail"]),
    )

    if sw["signal"] == "BUY" and sym_clean not in positions and n_pos < MAX_POSITIONS:
        qty = calc_qty(sw["price"], equity, sw["atr"], is_crypto)
        place_order(conn, symbol, OrderSide.BUY, qty, sw["price"],
                    f"SWING | {sw['detail']}")

    elif sw["signal"] == "SELL" and sym_clean in positions:
        qty = positions[sym_clean]
        place_order(conn, symbol, OrderSide.SELL, qty, sw["price"],
                    f"SWING exit | {sw['detail']}")

    conn.commit()

    # ── update latest bar table for dashboard sparklines ──────────────────────
    last_buf = list(bar_buffers.get(symbol, []))
    if last_buf:
        b = last_buf[-1]
        conn.execute(
            "INSERT OR REPLACE INTO bar_latest "
            "(symbol, ts, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (symbol, b["ts"], b["open"], b["high"], b["low"], b["close"], b["volume"]),
        )
        conn.commit()

# ── WebSocket bar handlers ─────────────────────────────────────────────────────
def make_crypto_handler(conn: sqlite3.Connection):
    async def on_crypto_bar(bar) -> None:
        sym = bar.symbol                        # e.g. "BTC/USD"
        buf = ensure_buffer(sym)
        buf.append({
            "ts": bar.timestamp.isoformat(),
            "open":   float(bar.open),
            "high":   float(bar.high),
            "low":    float(bar.low),
            "close":  float(bar.close),
            "volume": float(bar.volume),
        })
        log.debug(f"BAR  {sym}  close={bar.close:.2f}")
        process_bar(conn, sym, is_crypto=True)
    return on_crypto_bar

def make_stock_handler(conn: sqlite3.Connection):
    async def on_stock_bar(bar) -> None:
        sym = bar.symbol                        # e.g. "NVDA"
        buf = ensure_buffer(sym)
        buf.append({
            "ts": bar.timestamp.isoformat(),
            "open":   float(bar.open),
            "high":   float(bar.high),
            "low":    float(bar.low),
            "close":  float(bar.close),
            "volume": float(bar.volume),
        })
        log.debug(f"BAR  {sym}  close={bar.close:.2f}")
        process_bar(conn, sym, is_crypto=False)
    return on_stock_bar

# ── main loop with auto-reconnect ─────────────────────────────────────────────
async def run_streams(conn: sqlite3.Connection) -> None:
    db_set(conn, "engine_live", True)
    db_set(conn, "halted", False)
    db_set(conn, "session_high_equity", get_equity())

    while True:
        try:
            log.info("Starting WebSocket streams…")

            tasks = []

            # Crypto stream
            crypto_stream = CryptoDataStream(API_KEY, SECRET_KEY)
            crypto_stream.subscribe_bars(
                make_crypto_handler(conn),
                *CRYPTO_ASSETS,
            )
            tasks.append(asyncio.create_task(
                asyncio.to_thread(crypto_stream.run)
            ))

            # Equity stream (only when not crypto-only)
            if not CRYPTO_ONLY and EQUITY_ASSETS:
                stock_stream = StockDataStream(API_KEY, SECRET_KEY)
                stock_stream.subscribe_bars(
                    make_stock_handler(conn),
                    *EQUITY_ASSETS,
                )
                tasks.append(asyncio.create_task(
                    asyncio.to_thread(stock_stream.run)
                ))

            await asyncio.gather(*tasks)

        except Exception as e:
            log.error(f"Stream error: {e} — reconnecting in 10s…")
            db_set(conn, "engine_live", False)
            await asyncio.sleep(10)

def main() -> None:
    conn = get_db()
    db_set(conn, "engine_live", False)
    seed_buffers()
    log.info(f"Engine ready  |  DB: {DB_PATH}")
    log.info(f"Assets: crypto={CRYPTO_ASSETS}  equities={'OFF' if CRYPTO_ONLY else EQUITY_ASSETS}")
    asyncio.run(run_streams(conn))

if __name__ == "__main__":
    main()
