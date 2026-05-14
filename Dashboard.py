"""
EliteForge v36 — Dashboard
============================
Run alongside the engine:
    Terminal 1:  python stream_engine.py
    Terminal 2:  streamlit run dashboard.py

This file is READ-ONLY — it never touches Alpaca directly.
All trading logic lives in stream_engine.py.
"""

import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

DB_PATH = Path(__file__).parent / "engine.db"

# ── page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="EliteForge v36 • Live",
    layout="wide",
    page_icon="⚡",
)
st.markdown("""
<style>
    .stApp          { background:#04040a; color:#dde3ff;
                      font-family:'Segoe UI',sans-serif; }
    .title          { font-size:2.8rem; font-weight:900;
                      background:linear-gradient(90deg,#00f2ff,#7b2fff);
                      -webkit-background-clip:text;
                      -webkit-text-fill-color:transparent;
                      text-align:center; margin-bottom:.15rem; }
    .sub            { text-align:center; color:#555; font-size:.82rem;
                      margin-bottom:1.2rem; }
    .live-dot       { display:inline-block; width:9px; height:9px;
                      background:#00ff96; border-radius:50%;
                      margin-right:6px; animation:pulse 1.4s infinite; }
    .dead-dot       { display:inline-block; width:9px; height:9px;
                      background:#ff4444; border-radius:50%;
                      margin-right:6px; }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
    .status-bar     { display:flex; align-items:center; justify-content:center;
                      font-size:.82rem; color:#888; margin-bottom:1rem; gap:18px; }
    .halt-box       { background:rgba(255,40,40,.12); border:1px solid #ff2828;
                      border-radius:8px; padding:12px 16px; color:#ff4444;
                      font-weight:700; text-align:center; font-size:1rem; }
    .pill-buy       { background:rgba(0,255,150,.15); border:1px solid #00ff96;
                      color:#00ff96; padding:2px 10px; border-radius:20px;
                      font-size:.75rem; font-weight:700; }
    .pill-sell      { background:rgba(255,60,60,.15); border:1px solid #ff3c3c;
                      color:#ff3c3c; padding:2px 10px; border-radius:20px;
                      font-size:.75rem; font-weight:700; }
    .pill-hold      { background:rgba(150,150,150,.1); border:1px solid #444;
                      color:#777; padding:2px 10px; border-radius:20px;
                      font-size:.75rem; }
    .no-engine      { background:rgba(255,180,0,.08); border:1px solid #ffb400;
                      border-radius:8px; padding:14px; color:#ffb400;
                      text-align:center; }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="title">⚡ ELITEFORGE v36</div>', unsafe_allow_html=True)
st.markdown('<div class="sub">Real-Time Stream Dashboard</div>', unsafe_allow_html=True)

# ── DB helpers ─────────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn() -> sqlite3.Connection | None:
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def db_get(conn, key, default=None):
    try:
        row = conn.execute(
            "SELECT value FROM engine_state WHERE key=?", (key,)
        ).fetchone()
        return json.loads(row["value"]) if row else default
    except Exception:
        return default

conn = get_conn()

# ── engine status ──────────────────────────────────────────────────────────────
if conn is None:
    st.markdown(
        '<div class="no-engine">⚠️  <b>engine.db not found</b> — '
        'start the engine first:<br><code>python stream_engine.py</code></div>',
        unsafe_allow_html=True,
    )
    time.sleep(5)
    st.rerun()

engine_live = db_get(conn, "engine_live", False)
halted      = db_get(conn, "halted",      False)
equity      = db_get(conn, "equity",      0.0)
last_tick   = db_get(conn, "last_tick",   "—")
sess_high   = db_get(conn, "session_high_equity", equity)
drawdown    = (sess_high - equity) / sess_high * 100 if sess_high > 0 else 0.0

# Status bar
dot   = '<span class="live-dot"></span>' if engine_live else '<span class="dead-dot"></span>'
state = "LIVE" if engine_live else "OFFLINE"
st.markdown(
    f'<div class="status-bar">{dot}<b>{state}</b>'
    f'&nbsp;|&nbsp;Last bar: {last_tick}'
    f'&nbsp;|&nbsp;Session high: ${sess_high:,.2f}'
    f'&nbsp;|&nbsp;Drawdown: {drawdown:.2f}%</div>',
    unsafe_allow_html=True,
)

if halted:
    st.markdown(
        '<div class="halt-box">🛑 CIRCUIT BREAKER ACTIVE — '
        'Engine has halted all trading. Restart engine to reset.</div>',
        unsafe_allow_html=True,
    )

# ── tabs ───────────────────────────────────────────────────────────────────────
tab_terminal, tab_signals, tab_log = st.tabs(["⚡ Terminal", "🎯 Live Signals", "📜 Trade Log"])

# ── Terminal ───────────────────────────────────────────────────────────────────
with tab_terminal:
    c1, c2, c3, c4 = st.columns(4)

    # Pull account info from Alpaca (read-only)
    try:
        from alpaca.trading.client import TradingClient
        _key    = st.secrets["alpaca"]["api_key"]
        _secret = st.secrets["alpaca"]["secret_key"]

        @st.cache_resource
        def _trade_client():
            return TradingClient(_key, _secret, paper=True)

        _tc  = _trade_client()
        _acc = _tc.get_account()
        _eq  = float(_acc.equity)
        _bp  = float(_acc.buying_power)
        _pos = _tc.get_all_positions()
    except Exception as _e:
        st.warning(f"Alpaca read error: {_e}")
        _eq, _bp, _pos = equity, 0.0, []

    c1.metric("Equity",          f"${_eq:,.2f}")
    c2.metric("Buying Power",    f"${_bp:,.2f}")
    c3.metric("Open Positions",  len(_pos))
    c4.metric("Session Drawdown", f"{drawdown:.2f}%", delta_color="inverse")

    st.divider()
    st.subheader("📊 Open Positions")
    if _pos:
        rows = [{
            "Symbol": p.symbol,
            "Qty":    p.qty,
            "Entry":  f"${float(p.avg_entry_price):,.4f}",
            "Price":  f"${float(p.current_price):,.4f}",
            "Value":  f"${float(p.market_value):,.2f}",
            "PnL $":  f"${float(p.unrealized_pl):+,.2f}",
            "PnL %":  f"{(float(p.unrealized_pl)/float(p.cost_basis))*100:+.2f}%",
        } for p in _pos]
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.info("No open positions.")

    st.divider()
    st.subheader("📈 Latest Bar Prices")
    try:
        bars = conn.execute(
            "SELECT symbol, ts, open, high, low, close, volume FROM bar_latest ORDER BY symbol"
        ).fetchall()
        if bars:
            bar_rows = [{
                "Symbol": b["symbol"],
                "Time":   b["ts"],
                "Open":   f"${b['open']:,.4f}",
                "High":   f"${b['high']:,.4f}",
                "Low":    f"${b['low']:,.4f}",
                "Close":  f"${b['close']:,.4f}",
                "Volume": f"{b['volume']:,.0f}",
            } for b in bars]
            st.dataframe(pd.DataFrame(bar_rows), use_container_width=True, hide_index=True)
        else:
            st.info("Waiting for first bar…")
    except Exception as e:
        st.warning(f"Bar table not ready: {e}")

# ── Live Signals ───────────────────────────────────────────────────────────────
with tab_signals:
    col_scalp, col_swing = st.columns(2)

    def signal_badge(sig: str) -> str:
        if sig == "BUY":
            return "🟢 BUY"
        if sig == "SELL":
            return "🔴 SELL"
        return "⚪ HOLD"

    with col_scalp:
        st.subheader("🚀 Scalper  (1m)")
        try:
            rows = conn.execute("""
                SELECT s.symbol, s.signal, s.price, s.atr, s.detail, s.ts
                FROM signals s
                INNER JOIN (
                    SELECT symbol, MAX(id) AS max_id
                    FROM signals WHERE mode='SCALP'
                    GROUP BY symbol
                ) latest ON s.id = latest.max_id
                ORDER BY s.symbol
            """).fetchall()
            if rows:
                df_s = pd.DataFrame([{
                    "Symbol": r["symbol"],
                    "Signal": signal_badge(r["signal"]),
                    "Price":  f"${r['price']:,.4f}",
                    "ATR":    f"${r['atr']:.4f}",
                    "Detail": r["detail"],
                    "Time":   r["ts"][11:19],
                } for r in rows])
                st.dataframe(df_s, use_container_width=True, hide_index=True)
            else:
                st.info("Waiting for scalper signals…")
        except Exception as e:
            st.warning(f"Signal query error: {e}")

    with col_swing:
        st.subheader("🏛️ Swing  (1h resampled)")
        try:
            rows = conn.execute("""
                SELECT s.symbol, s.signal, s.price, s.atr, s.detail, s.ts
                FROM signals s
                INNER JOIN (
                    SELECT symbol, MAX(id) AS max_id
                    FROM signals WHERE mode='SWING'
                    GROUP BY symbol
                ) latest ON s.id = latest.max_id
                ORDER BY s.symbol
            """).fetchall()
            if rows:
                df_sw = pd.DataFrame([{
                    "Symbol": r["symbol"],
                    "Signal": signal_badge(r["signal"]),
                    "Price":  f"${r['price']:,.4f}",
                    "ATR":    f"${r['atr']:.4f}",
                    "Detail": r["detail"],
                    "Time":   r["ts"][11:19],
                } for r in rows])
                st.dataframe(df_sw, use_container_width=True, hide_index=True)
            else:
                st.info("Waiting for swing signals…")
        except Exception as e:
            st.warning(f"Signal query error: {e}")

    st.divider()
    st.subheader("📡 Signal History  (last 50)")
    try:
        hist = conn.execute(
            "SELECT ts, symbol, mode, signal, price, detail "
            "FROM signals ORDER BY id DESC LIMIT 50"
        ).fetchall()
        if hist:
            df_h = pd.DataFrame([{
                "Time":   r["ts"][11:19],
                "Symbol": r["symbol"],
                "Mode":   r["mode"],
                "Signal": signal_badge(r["signal"]),
                "Price":  f"${r['price']:,.4f}",
                "Detail": r["detail"],
            } for r in hist])
            st.dataframe(df_h, use_container_width=True, hide_index=True)
    except Exception as e:
        st.warning(f"History query error: {e}")

# ── Trade Log ──────────────────────────────────────────────────────────────────
with tab_log:
    st.subheader("📜 All Executed Trades")
    try:
        trades = conn.execute(
            "SELECT ts, symbol, side, qty, price, reason, status "
            "FROM trades ORDER BY id DESC LIMIT 200"
        ).fetchall()
        if trades:
            df_t = pd.DataFrame([{
                "Time":   r["ts"][11:19],
                "Symbol": r["symbol"],
                "Side":   r["side"],
                "Qty":    r["qty"],
                "Price":  f"${r['price']:,.4f}",
                "Reason": r["reason"],
                "Status": r["status"],
            } for r in trades])
            st.dataframe(df_t, use_container_width=True, hide_index=True)

            # summary metrics
            st.divider()
            ok_trades  = [t for t in trades if t["status"] == "OK"]
            err_trades = [t for t in trades if t["status"] == "ERROR"]
            buys  = [t for t in ok_trades if t["side"] == "BUY"]
            sells = [t for t in ok_trades if t["side"] == "SELL"]
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total Orders",   len(ok_trades))
            m2.metric("Buys",           len(buys))
            m3.metric("Sells",          len(sells))
            m4.metric("Errors",         len(err_trades))
        else:
            st.info("No trades recorded yet.")
    except Exception as e:
        st.warning(f"Trade log error: {e}")

    st.divider()
    st.caption(
        "All order execution happens in stream_engine.py. "
        "This dashboard is read-only — it queries engine.db every few seconds."
    )

# ── auto-refresh ───────────────────────────────────────────────────────────────
# Fast refresh since engine pushes data on every 1m bar close
time.sleep(5)
st.rerun()
