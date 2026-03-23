"""Live monitoring console for the market bot.

Run from the project root:
    streamlit run src/dashboard.py

The bot writes bot_state.json every 5s.  The dashboard reads it plus
opportunities.csv (only the last N rows — no full-file load).

Key design choices:
- @st.fragment(run_every=N) reruns only the affected section, not the whole
  page — no janky time.sleep() + st.rerun() full-page hammer.
- CSV reads use a deque-based tail that never loads more than TAIL_ROWS lines.
- Three tabs keep the page navigable as data grows.
"""

from __future__ import annotations

import io
import json
from collections import deque
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Config ─────────────────────────────────────────────────────────────────────
STATE_FILE = Path("bot_state.json")
CSV_FILE = Path("opportunities.csv")
TAIL_ROWS = 10_000  # max CSV rows loaded at once — never reads the whole file

st.set_page_config(
    page_title="Market Bot Console",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Helpers ────────────────────────────────────────────────────────────────────


def load_state() -> dict:
    """Load bot_state.json; return empty dict if missing or corrupt."""
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}


def read_csv_tail(n: int = TAIL_ROWS) -> pd.DataFrame:
    """Read the last n rows of opportunities.csv without loading the full file.

    Uses collections.deque(file, n) which iterates line-by-line but only keeps
    the last n lines in memory — safe even on 800k+ row files.
    """
    if not CSV_FILE.exists() or CSV_FILE.stat().st_size < 50:
        return pd.DataFrame()
    try:
        with CSV_FILE.open() as f:
            header = f.readline()
            lines = deque(f, n)
        raw = io.StringIO(header + "".join(lines))
        df = pd.read_csv(raw)
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        for col in ("net_profit", "gross_credit", "total_fees", "guaranteed_floor"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.dropna(subset=["net_profit", "timestamp"]).sort_values("timestamp")
    except Exception:
        return pd.DataFrame()


def bot_status(state: dict) -> tuple[bool, str]:
    """Returns (online, label) based on state file freshness."""
    if not state:
        return False, "OFFLINE"
    try:
        delta = (
            datetime.now().astimezone() - datetime.fromisoformat(state["updated_at"])
        ).total_seconds()
        if delta < 15:
            return True, "LIVE"
        return False, f"STALE ({int(delta)}s ago)"
    except Exception:
        return False, "OFFLINE"


# ── Page title (static — only renders once) ───────────────────────────────────
st.markdown("## 📈 Market Bot Console")
tab_live, tab_perf, tab_book = st.tabs(
    ["🟢  Live Monitor", "📊  Performance", "📋  Order Book & Health"]
)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — LIVE MONITOR
# ══════════════════════════════════════════════════════════════════════════════
with tab_live:

    @st.fragment(run_every="5s")
    def live_monitor() -> None:
        state = load_state()
        online, label = bot_status(state)

        # ── Status row ──────────────────────────────────────────────────────
        c_st, c_mo, c_up, c_thr, c_ts = st.columns(5)
        status_color = "🟢" if online else "🔴"
        c_st.metric("Status", f"{status_color} {label}")
        c_mo.metric("Mode", state.get("mode", "—"))
        uptime_s = int(state.get("uptime_s", 0))
        h, rem = divmod(uptime_s, 3600)
        m, s = divmod(rem, 60)
        c_up.metric("Uptime", f"{h}h {m:02d}m {s:02d}s")
        c_thr.metric("Min Profit", f"${state.get('min_profit_usd', 0):.2f}")
        c_ts.metric(
            "Last Update",
            datetime.fromisoformat(state["updated_at"]).strftime("%H:%M:%S")
            if state.get("updated_at")
            else "—",
        )

        # ── Throughput metrics ───────────────────────────────────────────────
        st.divider()
        m1, m2, m3, m4, m5, m6 = st.columns(6)
        m1.metric("Quotes Cached", f"{state.get('quote_count', 0):,}")
        m2.metric("Total Scans", f"{state.get('scan_count', 0):,}")
        m3.metric("Opps Found", f"{state.get('found_count', 0):,}")
        m4.metric("Opps Valid", f"{state.get('valid_count', 0):,}")
        m5.metric("Quote Queue", state.get("quote_queue_size", 0))
        m6.metric("Opp Queue", state.get("opportunity_queue_size", 0))

        # ── Market prices per underlying ─────────────────────────────────────
        underlyings: list[str] = state.get("underlyings", [])
        if underlyings:
            st.divider()
            st.markdown("**Market Prices**")
            idx = state.get("index_prices", {})
            perp = state.get("perp_marks", {})
            funding = state.get("perp_funding_rates", {})
            exp_c = state.get("expiry_counts", {})
            pcols = st.columns(len(underlyings) * 4)
            for i, u in enumerate(underlyings):
                base = i * 4
                pcols[base].metric(f"{u} Index", f"${idx.get(u, 0):,.2f}")
                pcols[base + 1].metric(f"{u} Perp", f"${perp.get(u, 0):,.2f}")
                pcols[base + 2].metric(f"{u} Funding/h", f"{funding.get(u, 0) * 100:.4f}%")
                pcols[base + 3].metric(f"{u} Expiries", exp_c.get(u, 0))

        # ── Recent valid opportunities ───────────────────────────────────────
        st.divider()
        st.markdown("**Recent Valid Opportunities** (live, last 50)")
        recent: list[dict] = state.get("recent_opportunities", [])
        if recent:
            rdf = pd.DataFrame(list(reversed(recent)))
            show = [
                c
                for c in [
                    "timestamp",
                    "arb_type",
                    "underlying",
                    "expiry",
                    "net_profit",
                    "gross_credit",
                    "total_fees",
                    "guaranteed_floor",
                ]
                if c in rdf.columns
            ]
            fmt = {
                c: "{:.4f}"
                for c in ["net_profit", "gross_credit", "total_fees", "guaranteed_floor"]
                if c in rdf.columns
            }
            st.dataframe(
                rdf[show].style.format(fmt),  # type: ignore[arg-type]
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.caption("No valid opportunities found yet — bot is scanning…")

    live_monitor()

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — PERFORMANCE CHARTS
# ══════════════════════════════════════════════════════════════════════════════
with tab_perf:

    @st.fragment(run_every="20s")
    def performance_charts() -> None:
        df = read_csv_tail()

        if df.empty:
            st.info(
                f"No data in {CSV_FILE} yet — the bot needs to find and validate "
                "opportunities before charts appear."
            )
            return

        df["cumulative_profit"] = df["net_profit"].cumsum()
        total_pnl = df["net_profit"].sum()

        # Summary strip
        s1, s2, s3, s4, s5 = st.columns(5)
        s1.metric("Total P&L", f"${total_pnl:,.2f}")
        s2.metric("Opportunities", f"{len(df):,}")
        s3.metric("Mean / opp", f"${df['net_profit'].mean():.4f}")
        s4.metric("Best opp", f"${df['net_profit'].max():.4f}")
        s5.metric("Worst opp", f"${df['net_profit'].min():.4f}")

        st.divider()

        # Row 1: Cumulative P&L + Arb type distribution
        ch1, ch2 = st.columns(2)
        with ch1:
            st.markdown("**Cumulative P&L (USD)**")
            fig = px.line(
                df, x="timestamp", y="cumulative_profit", color_discrete_sequence=["#00e676"]
            )
            fig.update_layout(
                margin=dict(l=0, r=0, t=10, b=0), height=300, xaxis_title=None, yaxis_title="USD"
            )
            st.plotly_chart(fig, use_container_width=True)

        with ch2:
            st.markdown("**Opportunity Type Distribution**")
            tc = df["arb_type"].value_counts().reset_index()
            tc.columns = ["type", "count"]
            fig2 = px.bar(
                tc,
                x="type",
                y="count",
                color="type",
                color_discrete_sequence=px.colors.qualitative.Pastel,
                text_auto=True,
            )
            fig2.update_layout(
                margin=dict(l=0, r=0, t=10, b=0), height=300, showlegend=False, xaxis_title=None
            )
            st.plotly_chart(fig2, use_container_width=True)

        # Row 2: Profit histogram + P&L per arb type (box plot)
        ch3, ch4 = st.columns(2)
        with ch3:
            st.markdown("**Profit Distribution**")
            fig3 = px.histogram(df, x="net_profit", nbins=80, color_discrete_sequence=["#448aff"])
            fig3.add_vline(
                x=float(df["net_profit"].mean()),
                line_dash="dash",
                line_color="orange",
                annotation_text="mean",
            )
            fig3.update_layout(
                margin=dict(l=0, r=0, t=10, b=0),
                height=280,
                xaxis_title="Net Profit (USD)",
                yaxis_title="Count",
            )
            st.plotly_chart(fig3, use_container_width=True)

        with ch4:
            st.markdown("**Profit by Arb Type**")
            fig4 = px.box(
                df,
                x="arb_type",
                y="net_profit",
                color="arb_type",
                color_discrete_sequence=px.colors.qualitative.Set2,
                points="outliers",
            )
            fig4.update_layout(
                margin=dict(l=0, r=0, t=10, b=0),
                height=280,
                showlegend=False,
                xaxis_title=None,
                yaxis_title="USD",
            )
            st.plotly_chart(fig4, use_container_width=True)

        # Row 3: P&L by underlying if multi-asset + P&L by expiry
        if "underlying" in df.columns:
            ch5, ch6 = st.columns(2)
            with ch5:
                if df["underlying"].nunique() > 1:
                    st.markdown("**Cumulative P&L by Underlying**")
                    df2 = df.copy()
                    df2["cum_by_u"] = df2.groupby("underlying")["net_profit"].cumsum()
                    fig5 = px.line(df2, x="timestamp", y="cum_by_u", color="underlying")
                    fig5.update_layout(
                        margin=dict(l=0, r=0, t=10, b=0),
                        height=260,
                        xaxis_title=None,
                        yaxis_title="USD",
                    )
                    st.plotly_chart(fig5, use_container_width=True)
                else:
                    # Profit over time scatter coloured by type
                    st.markdown("**Opportunity Profit Timeline**")
                    fig5 = px.scatter(
                        df,
                        x="timestamp",
                        y="net_profit",
                        color="arb_type",
                        opacity=0.7,
                        color_discrete_sequence=px.colors.qualitative.Pastel,
                    )
                    fig5.update_layout(
                        margin=dict(l=0, r=0, t=10, b=0),
                        height=260,
                        xaxis_title=None,
                        yaxis_title="USD",
                    )
                    st.plotly_chart(fig5, use_container_width=True)

            with ch6:
                if "expiry" in df.columns:
                    st.markdown("**Total P&L by Expiry**")
                    exp_pnl = (
                        df.groupby("expiry")["net_profit"]
                        .sum()
                        .reset_index()
                        .sort_values("net_profit", ascending=False)
                    )
                    fig6 = px.bar(
                        exp_pnl,
                        x="expiry",
                        y="net_profit",
                        color="net_profit",
                        color_continuous_scale=["#ff4444", "#ffaa00", "#00e676"],
                    )
                    fig6.update_layout(
                        margin=dict(l=0, r=0, t=10, b=0),
                        height=260,
                        xaxis_title=None,
                        coloraxis_showscale=False,
                    )
                    st.plotly_chart(fig6, use_container_width=True)

        st.caption(
            f"Showing last {len(df):,} rows of {CSV_FILE} (capped at {TAIL_ROWS:,} for performance)"
        )

    performance_charts()

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — ORDER BOOK & HEALTH
# ══════════════════════════════════════════════════════════════════════════════
with tab_book:

    @st.fragment(run_every="5s")
    def orderbook_health() -> None:
        state = load_state()

        # ── Live order book snapshot ─────────────────────────────────────────
        st.markdown("**Live Quotes — Sorted by Bid-Ask Spread % (top = most anomalous)**")
        snapshot: list[dict] = state.get("quotes_snapshot", [])
        if snapshot:
            sdf = pd.DataFrame(snapshot)
            show_cols = [
                c
                for c in [
                    "instrument",
                    "underlying",
                    "expiry",
                    "strike",
                    "type",
                    "bid",
                    "ask",
                    "spread_usd",
                    "spread_pct",
                    "mark",
                    "iv_pct",
                    "delta",
                    "age_ms",
                ]
                if c in sdf.columns
            ]
            st.dataframe(
                sdf[show_cols]
                .style.format(  # type: ignore[arg-type]
                    {
                        "bid": "{:.4f}",
                        "ask": "{:.4f}",
                        "spread_usd": "{:.4f}",
                        "spread_pct": "{:.2f}%",
                        "mark": "{:.4f}",
                        "iv_pct": "{:.1f}%",
                        "delta": "{:.3f}",
                        "age_ms": "{:,.0f} ms",
                    }
                )
                .background_gradient(  # type: ignore[attr-defined]
                    subset=["spread_pct"], cmap="RdYlGn_r"
                ),
                use_container_width=True,
                hide_index=True,
                height=350,
            )

            # IV distribution among the live quotes
            if "iv_pct" in sdf.columns and bool(sdf["iv_pct"].gt(0).any()):
                iv1, iv2 = st.columns(2)
                with iv1:
                    st.markdown("**IV Distribution (live quotes)**")
                    fig_iv = px.histogram(
                        sdf[sdf["iv_pct"] > 0],
                        x="iv_pct",
                        color="type" if "type" in sdf.columns else None,
                        nbins=50,
                        opacity=0.8,
                        color_discrete_sequence=px.colors.qualitative.Pastel,
                        barmode="overlay",
                    )
                    fig_iv.update_layout(
                        margin=dict(l=0, r=0, t=10, b=0),
                        height=260,
                        xaxis_title="IV %",
                        yaxis_title="Count",
                        showlegend=True,
                    )
                    st.plotly_chart(fig_iv, use_container_width=True)

                with iv2:
                    st.markdown("**Spread % Distribution (market liquidity)**")
                    fig_sp = px.histogram(
                        sdf,
                        x="spread_pct",
                        nbins=60,
                        color_discrete_sequence=["#ff7043"],
                    )
                    fig_sp.add_vline(
                        x=float(sdf["spread_pct"].median()),
                        line_dash="dash",
                        line_color="white",
                        annotation_text="median",
                    )
                    fig_sp.update_layout(
                        margin=dict(l=0, r=0, t=10, b=0),
                        height=260,
                        xaxis_title="Spread %",
                        yaxis_title="Count",
                    )
                    st.plotly_chart(fig_sp, use_container_width=True)
        else:
            st.caption("Order book snapshot not available — bot may not be running.")

        st.divider()

        # ── Scan rate chart ──────────────────────────────────────────────────
        scan_hist: list[dict] = state.get("scan_history", [])
        if scan_hist:
            shdf = pd.DataFrame(scan_hist)
            shdf["ts"] = pd.to_datetime(shdf["ts"])
            shdf["found"] = pd.to_numeric(shdf["found"], errors="coerce").fillna(0)  # type: ignore[union-attr]
            shdf["duration_ms"] = pd.to_numeric(shdf["duration_ms"], errors="coerce").fillna(0)  # type: ignore[union-attr]

            sc1, sc2 = st.columns(2)
            with sc1:
                st.markdown("**Opportunities Found per Scan (recent ~3 min)**")
                fig_scan = px.bar(
                    shdf.tail(200), x="ts", y="found", color_discrete_sequence=["#00e676"]
                )
                fig_scan.update_layout(
                    margin=dict(l=0, r=0, t=10, b=0),
                    height=240,
                    xaxis_title=None,
                    yaxis_title="Opps found",
                )
                st.plotly_chart(fig_scan, use_container_width=True)

            with sc2:
                st.markdown("**Scan Duration (ms)**")
                fig_dur = go.Figure()
                fig_dur.add_trace(
                    go.Scatter(
                        x=shdf["ts"],
                        y=shdf["duration_ms"],
                        mode="lines",
                        line=dict(color="#ffa726", width=1),
                        fill="tozeroy",
                        fillcolor="rgba(255,167,38,0.15)",
                    )
                )
                fig_dur.update_layout(
                    margin=dict(l=0, r=0, t=10, b=0),
                    height=240,
                    xaxis_title=None,
                    yaxis_title="ms",
                    plot_bgcolor="#0e1117",
                    paper_bgcolor="#0e1117",
                    font_color="#fafafa",
                )
                st.plotly_chart(fig_dur, use_container_width=True)

        st.divider()

        # ── Rejection reasons breakdown ──────────────────────────────────────
        rej: dict = state.get("rejection_counts", {})
        if rej:
            st.markdown("**Rejection Reasons**")
            rdf = pd.DataFrame(
                sorted(rej.items(), key=lambda x: x[1], reverse=True),
                columns=["Reason", "Count"],  # type: ignore[arg-type]
            )
            rc1, rc2 = st.columns([1, 2])
            with rc1:
                st.dataframe(rdf, hide_index=True, use_container_width=True)
            with rc2:
                fig_rej = px.pie(
                    rdf,
                    names="Reason",
                    values="Count",
                    color_discrete_sequence=px.colors.qualitative.Set3,
                    hole=0.4,
                )
                fig_rej.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=260)
                st.plotly_chart(fig_rej, use_container_width=True)
        else:
            found = state.get("found_count", 0)
            if found > 0:
                st.success(f"No rejections recorded — all {found} found opportunities passed risk.")
            else:
                st.caption("No rejection data yet.")

    orderbook_health()
