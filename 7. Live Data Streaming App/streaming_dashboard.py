"""
Live Data Streaming Dashboard — Streamlit app.

Launches an in-process producer + consumer and renders a self-refreshing
dashboard with:
  - Live price ticker cards for every symbol
  - Real-time candlestick / line charts
  - Volume bars
  - Moving-average overlays (SMA-20, SMA-50)
  - Alert log
  - Downloadable snapshot

Run:
    streamlit run streaming_dashboard.py
"""

import queue
import time
from collections import defaultdict
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from config import MAX_HISTORY, REFRESH_INTERVAL_MS, SYMBOLS
from consumer import InMemoryConsumer, StreamAnalytics
from producer import InMemoryProducer

# ---------------------------------------------------------------------------
# Page configuration
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Live Market Data Stream",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Session-state initialisation
# ---------------------------------------------------------------------------
def _init_state() -> None:
    """Create shared queues, producer, and consumer on first run."""
    if "initialised" in st.session_state:
        return

    raw_q: queue.Queue = queue.Queue(maxsize=2000)
    processed_q: queue.Queue = queue.Queue(maxsize=2000)

    # Sidebar controls are not yet rendered, use defaults
    symbols = SYMBOLS
    tick_interval = 0.3

    producer = InMemoryProducer(raw_q, symbols=symbols, tick_interval=tick_interval)
    consumer = InMemoryConsumer(raw_q, processed_q)

    producer.start()
    consumer.start()

    st.session_state.raw_q = raw_q
    st.session_state.processed_q = processed_q
    st.session_state.producer = producer
    st.session_state.consumer = consumer
    st.session_state.analytics = consumer.analytics

    # Per-symbol history: list of enriched tick dicts
    st.session_state.history: dict[str, list] = defaultdict(list)
    st.session_state.all_ticks: list[dict] = []
    st.session_state.alerts: list[dict] = []
    st.session_state.initialised = True


_init_state()


# ---------------------------------------------------------------------------
# Drain the processed queue into session-state history
# ---------------------------------------------------------------------------
def _drain_queue(max_items: int = 300) -> int:
    """Pull up to *max_items* enriched ticks into session-state history."""
    processed_q: queue.Queue = st.session_state.processed_q
    count = 0
    while count < max_items:
        try:
            tick = processed_q.get_nowait()
        except queue.Empty:
            break

        symbol = tick["symbol"]
        history = st.session_state.history[symbol]
        history.append(tick)

        # Trim to window size
        if len(history) > MAX_HISTORY:
            del history[: len(history) - MAX_HISTORY]

        st.session_state.all_ticks.append(tick)

        if tick.get("alert"):
            st.session_state.alerts.append(
                {
                    "time": tick["timestamp"],
                    "symbol": symbol,
                    "alert": tick["alert"],
                    "price": tick["price"],
                    "change_pct": tick["change_pct"],
                }
            )
            # Keep last 50 alerts
            if len(st.session_state.alerts) > 50:
                st.session_state.alerts = st.session_state.alerts[-50:]

        count += 1
    return count


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.title("⚙️ Controls")
    st.markdown("---")

    available_symbols = SYMBOLS
    selected_symbols = st.multiselect(
        "Symbols to display",
        options=available_symbols,
        default=available_symbols[:5],
    )

    chart_type = st.radio("Chart type", ["Line", "Candlestick"], horizontal=True)

    show_sma = st.checkbox("Show SMA-20 / SMA-50", value=True)
    show_volume = st.checkbox("Show volume bars", value=True)

    refresh_ms = st.slider(
        "Refresh interval (ms)",
        min_value=500,
        max_value=5000,
        value=REFRESH_INTERVAL_MS,
        step=250,
    )

    st.markdown("---")
    st.markdown("**Stream status**")
    producer_running = st.session_state.producer.is_running
    consumer_running = st.session_state.consumer.is_running
    st.markdown(
        f"Producer: {'🟢 running' if producer_running else '🔴 stopped'}  \n"
        f"Consumer: {'🟢 running' if consumer_running else '🔴 stopped'}"
    )

    total_ticks = sum(
        len(v) for v in st.session_state.history.values()
    )
    st.metric("Total ticks (session)", total_ticks)

    st.markdown("---")
    if st.session_state.all_ticks:
        df_export = pd.DataFrame(st.session_state.all_ticks)
        st.download_button(
            "⬇ Download snapshot (CSV)",
            data=df_export.to_csv(index=False),
            file_name=f"stream_snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
        )


# ---------------------------------------------------------------------------
# Main dashboard
# ---------------------------------------------------------------------------
st.title("📈 Live Market Data Streaming Dashboard")
st.caption(
    "Real-time price stream with rolling analytics · "
    f"Refreshes every {refresh_ms} ms"
)

_drain_queue()

if not selected_symbols:
    st.warning("Select at least one symbol in the sidebar.")
    st.stop()


# ---------------------------------------------------------------------------
# Ticker cards row
# ---------------------------------------------------------------------------
def _latest(symbol: str) -> dict:
    h = st.session_state.history.get(symbol, [])
    return h[-1] if h else {}


cols = st.columns(len(selected_symbols))
for col, sym in zip(cols, selected_symbols):
    tick = _latest(sym)
    if not tick:
        col.metric(sym, "—")
        continue
    price = tick["price"]
    chg = tick["change_pct"]
    col.metric(
        label=sym,
        value=f"${price:,.4f}",
        delta=f"{chg:+.4f}%",
        delta_color="normal",
    )


# ---------------------------------------------------------------------------
# Price charts
# ---------------------------------------------------------------------------
st.markdown("### Price Charts")

def _build_chart(symbol: str) -> go.Figure:
    history = st.session_state.history.get(symbol, [])
    if not history:
        fig = go.Figure()
        fig.update_layout(title=f"{symbol} — no data yet", height=350)
        return fig

    df = pd.DataFrame(history)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    rows = 2 if show_volume else 1
    row_heights = [0.7, 0.3] if show_volume else [1.0]
    fig = make_subplots(
        rows=rows,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=row_heights,
    )

    # Price trace
    if chart_type == "Candlestick" and len(df) >= 4:
        # Aggregate into ~5-tick OHLC candles for visual clarity
        df["candle"] = df.index // 5
        ohlc = df.groupby("candle").agg(
            open=("price", "first"),
            high=("price", "max"),
            low=("price", "min"),
            close=("price", "last"),
            ts=("timestamp", "last"),
        )
        fig.add_trace(
            go.Candlestick(
                x=ohlc["ts"],
                open=ohlc["open"],
                high=ohlc["high"],
                low=ohlc["low"],
                close=ohlc["close"],
                name=symbol,
                showlegend=False,
            ),
            row=1,
            col=1,
        )
    else:
        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["price"],
                mode="lines",
                name="Price",
                line=dict(color="#00b4d8", width=1.5),
            ),
            row=1,
            col=1,
        )

    # Moving averages
    if show_sma:
        sma20 = df["sma_20"].dropna()
        if not sma20.empty:
            fig.add_trace(
                go.Scatter(
                    x=df.loc[sma20.index, "timestamp"],
                    y=sma20,
                    mode="lines",
                    name="SMA-20",
                    line=dict(color="#f77f00", width=1, dash="dot"),
                ),
                row=1,
                col=1,
            )
        sma50 = df["sma_50"].dropna()
        if not sma50.empty:
            fig.add_trace(
                go.Scatter(
                    x=df.loc[sma50.index, "timestamp"],
                    y=sma50,
                    mode="lines",
                    name="SMA-50",
                    line=dict(color="#d62828", width=1, dash="dash"),
                ),
                row=1,
                col=1,
            )

    # VWAP
    vwap = df["vwap"].dropna()
    if not vwap.empty:
        fig.add_trace(
            go.Scatter(
                x=df.loc[vwap.index, "timestamp"],
                y=vwap,
                mode="lines",
                name="VWAP",
                line=dict(color="#80b918", width=1, dash="longdash"),
            ),
            row=1,
            col=1,
        )

    # Volume bars
    if show_volume:
        colors = [
            "#2dc653" if df["price"].iloc[i] >= df["price"].iloc[max(0, i - 1)] else "#e63946"
            for i in range(len(df))
        ]
        fig.add_trace(
            go.Bar(
                x=df["timestamp"],
                y=df["volume"],
                name="Volume",
                marker_color=colors,
                showlegend=False,
            ),
            row=2,
            col=1,
        )

    fig.update_layout(
        title=dict(text=f"<b>{symbol}</b>", x=0.02),
        height=400 if show_volume else 300,
        margin=dict(l=10, r=10, t=40, b=10),
        xaxis_rangeslider_visible=False,
        template="plotly_dark",
        legend=dict(orientation="h", y=1.05),
        hovermode="x unified",
    )
    return fig


# Two charts per row
for i in range(0, len(selected_symbols), 2):
    pair = selected_symbols[i : i + 2]
    chart_cols = st.columns(len(pair))
    for col, sym in zip(chart_cols, pair):
        col.plotly_chart(_build_chart(sym), use_container_width=True)


# ---------------------------------------------------------------------------
# Alert log
# ---------------------------------------------------------------------------
st.markdown("### 🔔 Alerts")
alerts = st.session_state.alerts
if alerts:
    df_alerts = pd.DataFrame(reversed(alerts)).head(10)
    df_alerts["time"] = pd.to_datetime(df_alerts["time"]).dt.strftime("%H:%M:%S.%f").str[:-3]
    df_alerts["change_pct"] = df_alerts["change_pct"].map("{:+.4f}%".format)
    st.dataframe(df_alerts, use_container_width=True, hide_index=True)
else:
    st.info("No alerts yet.  Alerts fire when a symbol moves ≥ 1.5% from its starting price.")


# ---------------------------------------------------------------------------
# Latest data table
# ---------------------------------------------------------------------------
with st.expander("📋 Latest ticks (all symbols)", expanded=False):
    rows = [_latest(s) for s in selected_symbols if _latest(s)]
    if rows:
        df_table = pd.DataFrame(rows)[
            ["symbol", "price", "bid", "ask", "volume", "change_pct", "sma_20", "vwap", "volatility", "timestamp"]
        ]
        st.dataframe(df_table, use_container_width=True, hide_index=True)
    else:
        st.info("Waiting for data…")


# ---------------------------------------------------------------------------
# Auto-refresh
# ---------------------------------------------------------------------------
time.sleep(refresh_ms / 1000)
st.rerun()
