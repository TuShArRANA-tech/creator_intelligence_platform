"""
Creator Intelligence Platform — Streamlit Dashboard (Redesigned).

Multi-page dashboard with sidebar navigation and Plotly visualizations.
Data is loaded directly from MySQL (`videos_cleaned`) via `database/db_connection.py`.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure the project root is importable regardless of cwd.
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from datetime import datetime
from typing import Optional

from database.db_connection import get_mysql_connection
import base64

# ─── Logo (base64-encoded for inline rendering) ─────────────────────────────────
_LOGO_PATH = Path(__file__).resolve().parent / "logo.png"
if _LOGO_PATH.exists():
    _LOGO_B64 = base64.b64encode(_LOGO_PATH.read_bytes()).decode()
    _LOGO_HTML = f'<img src="data:image/png;base64,{_LOGO_B64}" style="width:44px;height:44px;border-radius:12px;object-fit:cover;" />'
    _LOGO_HERO = f'<img src="data:image/png;base64,{_LOGO_B64}" style="width:38px;height:38px;border-radius:10px;object-fit:cover;vertical-align:middle;margin-right:10px;" />'
else:
    _LOGO_HTML = '🎯'
    _LOGO_HERO = '🎯'

# ─── Page Config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Creator Intelligence Platform",
    page_icon="🎯",
    layout="wide",
)

# ─── Design Tokens ──────────────────────────────────────────────────────────────
VIOLET     = "#8B5CF6"
CYAN       = "#22D3EE"
AMBER      = "#FBBF24"
EMERALD    = "#34D399"
ROSE       = "#FB7185"
SLATE_50   = "#F8FAFC"
SLATE_200  = "#E2E8F0"
SLATE_400  = "#94A3B8"
SLATE_600  = "#475569"
SLATE_800  = "#1E293B"
SLATE_900  = "#0F172A"
SURFACE    = "#161B2E"
CARD       = "#1E2640"
BORDER     = "rgba(139,92,246,0.18)"
GLOW       = "rgba(139,92,246,0.12)"

PALETTE = [VIOLET, CYAN, AMBER, EMERALD, ROSE, "#A78BFA", "#38BDF8", "#FB923C"]
CHART_H = 420

# ─── Global CSS ─────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');

    /* ── Reset & Base ────────────────────────────── */
    *, *::before, *::after { font-family: 'Inter', sans-serif !important; box-sizing: border-box; }

    .stApp {
        background: linear-gradient(168deg, #0B0F1A 0%, #101729 40%, #0F172A 100%);
        color: #E2E8F0;
    }
    [data-testid="stHeader"] { background: transparent !important; }

    /* ── Main Container ──────────────────────────── */
    [data-testid="stAppViewContainer"] .main .block-container {
        max-width: 1280px;
        padding: 1.5rem 2rem 2rem;
    }

    /* ── Sidebar ─────────────────────────────────── */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #111827 0%, #0D1321 100%);
        border-right: 1px solid rgba(139,92,246,0.12);
    }
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
        color: #CBD5E1;
    }

    /* ── Hero Banner ─────────────────────────────── */
    .hero {
        position: relative;
        overflow: hidden;
        border-radius: 20px;
        padding: 2rem 2.25rem 1.75rem;
        margin-bottom: 1.75rem;
        background: linear-gradient(135deg, rgba(139,92,246,0.15) 0%, rgba(34,211,238,0.08) 100%);
        border: 1px solid rgba(139,92,246,0.2);
        backdrop-filter: blur(24px);
    }
    .hero::before {
        content: "";
        position: absolute;
        top: -50%;
        right: -20%;
        width: 400px;
        height: 400px;
        background: radial-gradient(circle, rgba(139,92,246,0.12) 0%, transparent 70%);
        pointer-events: none;
    }
    .hero-title {
        margin: 0;
        font-size: 2rem;
        font-weight: 800;
        letter-spacing: -0.5px;
        background: linear-gradient(135deg, #FFFFFF 30%, #C4B5FD 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .hero-sub {
        margin: 0.3rem 0 0;
        font-size: 0.95rem;
        color: #94A3B8;
        font-weight: 400;
    }
    .hero-badge {
        display: inline-flex; align-items: center; gap: 6px;
        margin-top: 0.65rem;
        padding: 0.25rem 0.7rem;
        border-radius: 999px;
        background: rgba(52,211,153,0.12);
        border: 1px solid rgba(52,211,153,0.35);
        font-size: 0.75rem;
        font-weight: 600;
        color: #6EE7B7;
        letter-spacing: 0.6px;
        text-transform: uppercase;
    }
    .hero-dot {
        width: 6px; height: 6px;
        border-radius: 50%;
        background: #34D399;
        box-shadow: 0 0 6px rgba(52,211,153,0.6);
        animation: livePulse 2s ease-in-out infinite;
    }
    @keyframes livePulse {
        0%, 100% { opacity: 1; transform: scale(1); }
        50% { opacity: 0.4; transform: scale(0.85); }
    }

    /* ── Section Headers ─────────────────────────── */
    .sec-head {
        display: flex; align-items: center; gap: 8px;
        margin: 1.4rem 0 0.8rem;
        font-size: 1.05rem;
        font-weight: 700;
        color: #F1F5F9;
    }
    .sec-head::after {
        content: "";
        flex: 1;
        height: 1px;
        background: linear-gradient(90deg, rgba(139,92,246,0.3) 0%, transparent 100%);
    }

    /* ── Metric Cards ────────────────────────────── */
    .m-card {
        position: relative;
        border-radius: 16px;
        padding: 1.35rem 1.25rem 1.15rem;
        min-height: 130px;
        background: rgba(30,38,64,0.55);
        backdrop-filter: blur(16px);
        border: 1px solid rgba(139,92,246,0.12);
        overflow: hidden;
        transition: transform 0.25s ease, box-shadow 0.25s ease;
    }
    .m-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 20px 40px rgba(0,0,0,0.25);
    }
    .m-card::before {
        content: "";
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 3px;
        border-radius: 16px 16px 0 0;
    }
    .m-card-v::before { background: linear-gradient(90deg, #8B5CF6, #A78BFA); }
    .m-card-c::before { background: linear-gradient(90deg, #22D3EE, #38BDF8); }
    .m-card-a::before { background: linear-gradient(90deg, #FBBF24, #FB923C); }
    .m-card-e::before { background: linear-gradient(90deg, #34D399, #2DD4BF); }
    .m-icon {
        font-size: 1.5rem;
        margin-bottom: 0.65rem;
        display: block;
    }
    .m-val {
        font-size: 1.65rem;
        font-weight: 800;
        color: #FFFFFF;
        line-height: 1.2;
    }
    .m-label {
        font-size: 0.78rem;
        font-weight: 500;
        color: #94A3B8;
        margin-top: 0.35rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }

    /* ── Chart Wrapper ───────────────────────────── */
    .chart-wrap {
        background: rgba(30,38,64,0.4);
        backdrop-filter: blur(12px);
        border: 1px solid rgba(139,92,246,0.1);
        border-radius: 16px;
        padding: 0.5rem 0.6rem 0.2rem;
        margin-bottom: 0.75rem;
    }
    [data-testid="stPlotlyChart"] {
        background: transparent;
        border: 0;
    }

    /* ── Insight Cards ───────────────────────────── */
    .ins-card {
        background: rgba(30,38,64,0.45);
        border: 1px solid rgba(139,92,246,0.12);
        border-left: 3px solid #8B5CF6;
        border-radius: 12px;
        padding: 0.75rem 1rem;
        margin-bottom: 0.5rem;
        font-size: 0.9rem;
        color: #CBD5E1;
        transition: border-color 0.2s ease, background 0.2s ease;
    }
    .ins-card:hover {
        border-color: rgba(139,92,246,0.35);
        background: rgba(30,38,64,0.65);
    }
    .ins-card b { color: #E2E8F0; }

    /* ── Best Time Box ───────────────────────────── */
    .best-box {
        background: rgba(52,211,153,0.08);
        border: 1px solid rgba(52,211,153,0.25);
        border-radius: 14px;
        padding: 1.1rem 1.25rem;
        margin-bottom: 1rem;
    }
    .best-box-label {
        font-size: 0.7rem;
        font-weight: 700;
        letter-spacing: 1.2px;
        color: #6EE7B7;
        text-transform: uppercase;
        margin-bottom: 0.25rem;
    }
    .best-box-value {
        font-size: 1.6rem;
        font-weight: 800;
        color: #FFFFFF;
    }
    .best-box-sub {
        font-size: 0.85rem;
        color: #94A3B8;
        margin-top: 0.15rem;
    }

    /* ── Summary Banner ──────────────────────────── */
    .sum-banner {
        background: rgba(139,92,246,0.07);
        border: 1px solid rgba(139,92,246,0.18);
        border-radius: 12px;
        padding: 0.75rem 1rem;
        margin-bottom: 1.25rem;
        font-size: 0.9rem;
        color: #CBD5E1;
    }
    .sum-banner b { color: #E2E8F0; }

    /* ── Result Badge ────────────────────────────── */
    .res-badge {
        display: inline-flex; align-items: center; gap: 6px;
        background: rgba(139,92,246,0.1);
        border: 1px solid rgba(139,92,246,0.25);
        color: #C4B5FD;
        border-radius: 999px;
        padding: 0.3rem 0.75rem;
        font-size: 0.82rem;
        font-weight: 600;
        margin-bottom: 0.6rem;
    }

    /* ── Filter Caption ──────────────────────────── */
    .filt-cap {
        font-size: 0.78rem;
        color: #64748B;
        margin-bottom: -0.15rem;
    }

    /* ── Sidebar Branding ────────────────────────── */
    .sb-logo {
        width: 56px; height: 56px;
        margin: 0 auto 0.5rem;
        display: flex; align-items: center; justify-content: center;
        border-radius: 14px;
        background: linear-gradient(135deg, rgba(139,92,246,0.2) 0%, rgba(34,211,238,0.1) 100%);
        border: 1px solid rgba(139,92,246,0.3);
        font-size: 1.6rem;
    }
    .sb-name {
        text-align: center;
        font-size: 0.95rem;
        font-weight: 700;
        color: #F1F5F9;
        margin-bottom: 0.15rem;
    }
    .sb-stats {
        text-align: center;
        font-size: 0.78rem;
        color: #64748B;
        margin-bottom: 0.6rem;
    }
    .sb-divider {
        border: none;
        border-top: 1px solid rgba(139,92,246,0.1);
        margin: 0.6rem 0 0.8rem;
    }
    .sb-section {
        font-size: 0.68rem;
        font-weight: 700;
        letter-spacing: 1px;
        color: #64748B;
        text-transform: uppercase;
        margin-bottom: 0.35rem;
    }
    .sb-foot {
        margin-top: 1.5rem;
        text-align: center;
    }
    .sb-badge {
        display: inline-block;
        background: rgba(139,92,246,0.1);
        border: 1px solid rgba(139,92,246,0.2);
        color: #A78BFA;
        border-radius: 999px;
        padding: 0.15rem 0.45rem;
        font-size: 0.68rem;
        font-weight: 600;
        margin: 0.1rem;
    }
    .sb-powered {
        font-size: 0.75rem;
        color: #475569;
        margin-top: 0.6rem;
    }

    /* ── Radio buttons / Nav ─────────────────────── */
    div[role="radiogroup"] > label {
        background: rgba(30,38,64,0.35);
        border: 1px solid rgba(139,92,246,0.12);
        border-radius: 10px;
        margin-bottom: 0.3rem;
        padding: 0.45rem 0.65rem;
        transition: all 0.2s ease;
        font-size: 0.88rem;
    }
    div[role="radiogroup"] > label:hover {
        background: rgba(139,92,246,0.12);
        border-color: rgba(139,92,246,0.3);
    }
    div[role="radiogroup"] > label:has(input:checked) {
        background: rgba(139,92,246,0.18);
        border-color: rgba(139,92,246,0.5);
        box-shadow: 0 0 0 1px rgba(139,92,246,0.25) inset;
    }

    /* ── Tabs ────────────────────────────────────── */
    button[data-baseweb="tab"] {
        border-radius: 10px !important;
        border: 1px solid rgba(139,92,246,0.15) !important;
        background: rgba(30,38,64,0.3) !important;
        color: #94A3B8 !important;
        transition: all 0.2s ease !important;
        font-weight: 500 !important;
    }
    button[data-baseweb="tab"]:hover {
        background: rgba(139,92,246,0.1) !important;
        border-color: rgba(139,92,246,0.3) !important;
        color: #E2E8F0 !important;
    }
    button[data-baseweb="tab"][aria-selected="true"] {
        background: rgba(139,92,246,0.18) !important;
        border-color: rgba(139,92,246,0.5) !important;
        color: #FFFFFF !important;
        font-weight: 600 !important;
    }

    /* ── Footer ──────────────────────────────────── */
    .app-footer {
        margin-top: 2rem;
        padding: 0.85rem 1.25rem;
        border-radius: 12px;
        background: rgba(30,38,64,0.3);
        border: 1px solid rgba(139,92,246,0.08);
        display: flex;
        justify-content: space-between;
        align-items: center;
        font-size: 0.78rem;
        color: #475569;
    }
    .app-footer b { color: #64748B; }

    /* ── Performance Tier Badges ──────────────── */
    .tier-viral  { background: rgba(139,92,246,0.18); color:#C4B5FD; border:1px solid rgba(139,92,246,0.35); border-radius:8px; padding:2px 8px; font-weight:600; }
    .tier-high   { background: rgba(59,130,246,0.15); color:#93C5FD; border:1px solid rgba(59,130,246,0.35); border-radius:8px; padding:2px 8px; font-weight:600; }
    .tier-medium { background: rgba(251,191,36,0.15); color:#FDE68A; border:1px solid rgba(251,191,36,0.35); border-radius:8px; padding:2px 8px; font-weight:600; }
    .tier-low    { background: rgba(148,163,184,0.12); color:#CBD5E1; border:1px solid rgba(148,163,184,0.25); border-radius:8px; padding:2px 8px; font-weight:600; }

    /* ── Responsive ──────────────────────────────── */
    @media (max-width: 1200px) {
        .hero-title { font-size: 1.65rem; }
        .m-val { font-size: 1.35rem; }
        .m-card { min-height: 110px; padding: 1rem; }
    }
    @media (max-width: 768px) {
        [data-testid="stAppViewContainer"] .main .block-container {
            padding: 1rem;
        }
        .hero { padding: 1.25rem; border-radius: 14px; }
        .hero-title { font-size: 1.35rem; }
        .m-card { min-height: 96px; padding: 0.85rem; }
        .m-val { font-size: 1.15rem; }
        .app-footer { flex-direction: column; text-align: center; gap: 0.3rem; }
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ─── UI Helpers ─────────────────────────────────────────────────────────────────

def section_header(text: str) -> None:
    """Render a minimal section header with a gradient trailing line."""
    st.markdown(f'<div class="sec-head">{text}</div>', unsafe_allow_html=True)


def metric_card(icon: str, label: str, value: str, variant: str = "v") -> None:
    """Render a glassmorphic metric card with a coloured top-bar."""
    st.markdown(
        f"""
        <div class="m-card m-card-{variant}">
            <span class="m-icon">{icon}</span>
            <div class="m-val">{value}</div>
            <div class="m-label">{label}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def styled_df(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    """Dark-themed DataFrame styling with alternating row colours."""
    return (
        df.style.set_properties(
            **{
                "background-color": CARD,
                "color": SLATE_200,
                "border": f"1px solid rgba(139,92,246,0.08)",
            }
        )
        .set_table_styles(
            [
                {"selector": "th", "props": [("background-color", "#1A2035"), ("color", SLATE_200), ("font-weight", "600"), ("font-size", "0.82rem")]},
                {"selector": "td", "props": [("font-size", "0.84rem")]},
            ]
        )
        .apply(
            lambda row: [
                "background-color: #1A2035" if row.name % 2 == 0 else f"background-color: {CARD}"
                for _ in row
            ],
            axis=1,
        )
    )


def styled_leaderboard(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    """Leaderboard table with medal-row highlighting."""
    s = styled_df(df)
    if "rank" in df.columns:
        s = s.apply(
            lambda col: [
                "background-color:#5B21B6;color:#fff;font-weight:700;border-radius:6px;" if isinstance(v, str) and "🥇" in v
                else "background-color:#0891B2;color:#fff;font-weight:700;border-radius:6px;" if isinstance(v, str) and "🥈" in v
                else "background-color:#D97706;color:#1f2937;font-weight:700;border-radius:6px;" if isinstance(v, str) and "🥉" in v
                else "background-color:#1A2035;color:#E2E8F0;"
                for v in col
            ],
            subset=["rank"],
        )
    return s


def styled_explorer(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    """Explorer table with colour-coded performance tier badges."""
    s = styled_df(df)
    if "performance_tier" in df.columns:
        s = s.apply(
            lambda col: [
                "background-color:rgba(139,92,246,0.18);color:#C4B5FD;font-weight:600;" if str(v) == "Viral"
                else "background-color:rgba(59,130,246,0.15);color:#93C5FD;font-weight:600;" if str(v) == "High"
                else "background-color:rgba(251,191,36,0.15);color:#FDE68A;font-weight:600;" if str(v) == "Medium"
                else "background-color:rgba(148,163,184,0.12);color:#CBD5E1;font-weight:600;" if str(v) == "Low"
                else ""
                for v in col
            ],
            subset=["performance_tier"],
        )
    return s


def chart_theme(fig):
    """Apply a refined dark chart theme."""
    fig.update_layout(
        template="plotly_dark",
        height=CHART_H,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(30,38,64,0.25)",
        font=dict(color=SLATE_200, size=12, family="Inter"),
        hoverlabel=dict(bgcolor="#1E2640", font_color=SLATE_200, bordercolor="rgba(139,92,246,0.2)"),
        xaxis=dict(gridcolor="rgba(255,255,255,0.04)", zerolinecolor="rgba(255,255,255,0.04)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.04)", zerolinecolor="rgba(255,255,255,0.04)"),
        margin=dict(l=40, r=20, t=50, b=40),
        transition=dict(duration=500, easing="cubic-in-out"),
    )
    fig.update_xaxes(showline=False, mirror=False, zeroline=False)
    fig.update_yaxes(showline=False, mirror=False, zeroline=False)
    return fig


def show_chart(fig) -> None:
    """Render a Plotly chart inside a styled wrapper."""
    st.markdown('<div class="chart-wrap">', unsafe_allow_html=True)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    st.markdown('</div>', unsafe_allow_html=True)


# ─── Data Loading ───────────────────────────────────────────────────────────────

@st.cache_data
def load_videos_cleaned() -> pd.DataFrame:
    """Load cleaned dataset from MySQL (`videos_cleaned`) into a DataFrame."""
    conn = get_mysql_connection()
    try:
        return pd.read_sql("SELECT * FROM videos_cleaned", conn)
    finally:
        conn.close()


@st.cache_data
def load_last_updated() -> Optional[datetime]:
    """Fetch the latest `collected_at` timestamp from `videos_cleaned`."""
    conn = get_mysql_connection()
    try:
        df = pd.read_sql(
            "SELECT MAX(collected_at) AS last_updated FROM videos_cleaned", conn,
        )
        return df["last_updated"].iloc[0] if not df.empty else None
    finally:
        conn.close()


def safe_num(series: pd.Series) -> pd.Series:
    """Convert a series to numeric, coercing errors to NaN."""
    return pd.to_numeric(series, errors="coerce")


# ─── Pages ──────────────────────────────────────────────────────────────────────

def page_overview(df: pd.DataFrame) -> None:
    """Overview page — KPIs, distribution charts, and top insights."""

    if df.empty:
        st.info("No data available. Run collection & cleaning first.")
        return

    st.markdown(
        '<div class="sum-banner">📊 <b>Platform Summary</b> — Performance snapshot across videos, engagement, and category momentum.</div>',
        unsafe_allow_html=True,
    )

    total_videos = len(df)
    avg_eng = float(safe_num(df["engagement_rate"]).mean())
    total_views = int(safe_num(df["view_count"]).sum())
    top_cat = (
        df["category"].value_counts().idxmax()
        if "category" in df.columns and not df.empty else "N/A"
    )

    c1, c2, c3, c4 = st.columns(4, gap="medium")
    with c1:
        metric_card("📹", "Total Videos", f"{total_videos:,}", "v")
    with c2:
        metric_card("⚡", "Avg Engagement", f"{avg_eng:.4f}%", "c")
    with c3:
        metric_card("👁️", "Total Views", f"{total_views:,}", "a")
    with c4:
        metric_card("🏷️", "Top Category", str(top_cat), "e")

    # ── Charts ──
    cat_counts = df["category"].value_counts().reset_index()
    cat_counts.columns = ["category", "count"]

    cat_eng = (
        df.groupby("category", as_index=False)
        .agg(avg_engagement=("engagement_rate", "mean"))
        .sort_values("avg_engagement", ascending=False)
    )

    tier_counts = df["performance_tier"].value_counts().reset_index()
    tier_counts.columns = ["performance_tier", "count"]

    left, right = st.columns((1.3, 1), gap="medium")
    with left:
        section_header("📦 Videos by Category")
        fig = px.bar(
            cat_counts, x="category", y="count",
            color="category", color_discrete_sequence=PALETTE,
            title="Video Count by Category",
        )
        chart_theme(fig)
        fig.update_layout(showlegend=False)
        show_chart(fig)
    with right:
        section_header("🎯 Performance Tiers")
        fig2 = px.pie(
            tier_counts, names="performance_tier", values="count",
            hole=0.52, color="performance_tier",
            color_discrete_sequence=PALETTE,
            title="Performance Tier Distribution",
        )
        chart_theme(fig2)
        fig2.update_traces(textinfo="percent+label", textfont_size=11)
        show_chart(fig2)

    section_header("📈 Avg Engagement by Category")
    fig3 = px.bar(
        cat_eng, x="category", y="avg_engagement",
        color="category", color_discrete_sequence=PALETTE,
        title="Average Engagement Rate by Category",
    )
    chart_theme(fig3)
    fig3.update_layout(showlegend=False)
    show_chart(fig3)

    # ── Insights ──
    section_header("💡 Key Insights")
    best_cat_row = cat_eng.head(1)
    best_cat = best_cat_row["category"].iloc[0] if not best_cat_row.empty else "N/A"
    best_eng = float(best_cat_row["avg_engagement"].iloc[0]) if not best_cat_row.empty else 0.0
    best_hour_df = (
        df.groupby("upload_hour", as_index=False)
        .agg(avg_views=("view_count", "mean"))
        .sort_values("avg_views", ascending=False)
        .head(1)
    )
    best_hour = int(best_hour_df["upload_hour"].iloc[0]) if not best_hour_df.empty else 0
    benchmark = 4.50
    delta = avg_eng - benchmark
    direction = "above" if delta >= 0 else "below"

    st.markdown(
        f"""
        <div class="ins-card">📌 Top category: <b>{best_cat}</b> with <b>{best_eng:.4f}%</b> avg engagement rate.</div>
        <div class="ins-card">⏰ Peak upload hour: <b>{best_hour}:00</b> — highest average views.</div>
        <div class="ins-card">📊 Platform avg is <b>{abs(delta):.2f}%</b> {direction} the <b>{benchmark:.2f}%</b> reference benchmark.</div>
        """,
        unsafe_allow_html=True,
    )


def page_timing(df: pd.DataFrame) -> None:
    """Timing Intelligence page — heatmap, hourly line, day ranking."""

    if df.empty:
        st.info("No data available.")
        return

    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    df2 = df.copy()
    df2["upload_day"] = df2["upload_day"].astype(str)

    heatmap = (
        df2.groupby(["upload_day", "upload_hour"], as_index=False)
        .agg(avg_engagement=("engagement_rate", "mean"))
    )

    best = heatmap.sort_values("avg_engagement", ascending=False).head(1)
    if not best.empty:
        bd, bh, bv = best["upload_day"].iloc[0], int(best["upload_hour"].iloc[0]), float(best["avg_engagement"].iloc[0])
        st.markdown(
            f"""
            <div class="best-box">
                <div class="best-box-label">● Best Time to Post</div>
                <div class="best-box-value">{bd} · {bh}:00</div>
                <div class="best-box-sub">Avg engagement: {bv:.4f}%</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    pivot = (
        heatmap.pivot(index="upload_day", columns="upload_hour", values="avg_engagement")
        .reindex(day_order)
    )

    section_header("🗺️ Engagement Heatmap")
    fig = px.imshow(
        pivot, aspect="auto",
        title="Upload Day × Hour — Avg Engagement Rate",
        labels={"x": "Hour", "y": "Day", "color": "Engagement"},
        color_continuous_scale=["#1E293B", VIOLET, CYAN],
    )
    chart_theme(fig)
    show_chart(fig)

    hourly = (
        df2.groupby("upload_hour", as_index=False)
        .agg(avg_views=("view_count", "mean"))
        .sort_values("upload_hour")
    )

    section_header("📈 Average Views by Hour")
    fig2 = px.area(
        hourly, x="upload_hour", y="avg_views",
        title="Avg Views by Upload Hour",
    )
    fig2.update_traces(
        line=dict(color=CYAN, width=2.5),
        fillcolor="rgba(34,211,238,0.08)",
    )
    chart_theme(fig2)
    show_chart(fig2)

    section_header("📅 Day-of-Week Ranking")
    day_rank = (
        df2.groupby("upload_day", as_index=False)
        .agg(avg_engagement=("engagement_rate", "mean"), avg_views=("view_count", "mean"))
        .sort_values("avg_engagement", ascending=False)
        .reset_index(drop=True)
    )
    day_rank.insert(0, "rank", [f"#{i+1}" for i in range(len(day_rank))])
    st.dataframe(styled_df(day_rank), use_container_width=True, hide_index=True)


def page_leaderboard(df: pd.DataFrame) -> None:
    """Creator Leaderboard page — channel ranking by engagement."""

    if df.empty:
        st.info("No data available.")
        return

    categories = sorted(df["category"].dropna().unique().tolist())
    tier = st.selectbox("🏷️ Filter by category", ["All"] + categories, index=0)
    df2 = df if tier == "All" else df[df["category"] == tier]

    leaders = (
        df2.groupby(["channel_id", "channel_name"], as_index=False)
        .agg(
            avg_engagement=("engagement_rate", "mean"),
            total_views=("view_count", "sum"),
            video_count=("video_id", "count") if "video_id" in df2.columns else ("view_count", "size"),
        )
        .sort_values("avg_engagement", ascending=False)
        .head(20)
    )

    section_header("🏅 Top Channels — Avg Engagement")
    display = leaders.copy()
    medals = ["🥇", "🥈", "🥉"]
    display.insert(0, "rank", [
        f"{medals[i]} #{i+1}" if i < 3 else f"#{i+1}" for i in range(len(display))
    ])
    st.dataframe(styled_leaderboard(display), use_container_width=True, hide_index=True)

    top_views = (
        df2.groupby(["channel_id", "channel_name"], as_index=False)
        .agg(total_views=("view_count", "sum"))
        .sort_values("total_views", ascending=False)
        .head(10)
    )

    section_header("👁️ Top 10 by Total Views")
    fig = px.bar(
        top_views.sort_values("total_views", ascending=True),
        x="total_views", y="channel_name", orientation="h",
        title="Top 10 Channels by Total Views",
        color="channel_name", color_discrete_sequence=PALETTE,
    )
    chart_theme(fig)
    fig.update_layout(showlegend=False)
    show_chart(fig)

    section_header("✨ Rising Stars (3+ videos)")
    rising = leaders[leaders["video_count"] >= 3].sort_values(
        ["avg_engagement", "video_count"], ascending=[False, False]
    ).head(5)
    if not rising.empty:
        st.dataframe(styled_df(rising), use_container_width=True, hide_index=True)
    else:
        st.caption("No creators with 3+ videos in this filter.")


def page_explorer(df: pd.DataFrame) -> None:
    """Video Explorer page — searchable, filterable video table."""

    if df.empty:
        st.info("No data available.")
        return

    categories = sorted(df["category"].dropna().unique().tolist())
    tiers = ["Viral", "High", "Medium", "Low"]

    search = st.text_input("🔍 Search title", placeholder="Type to filter…")
    f1, f2 = st.columns(2, gap="medium")
    with f1:
        sel_cats = st.multiselect("🏷️ Category", categories, default=categories)
    with f2:
        sel_tiers = st.multiselect("🚦 Performance Tier", tiers, default=tiers)

    hr = st.slider("🕐 Upload hour range", 0, 23, (0, 23))

    max_v_val = pd.to_numeric(df["view_count"], errors="coerce").max() if not df.empty else 0
    max_views = int(max_v_val) if pd.notna(max_v_val) else 0
    min_views = st.slider("👁️ Minimum views", 0, max(max_views, 1), 0)

    df2 = df.copy()
    if search:
        df2 = df2[df2["title"].astype(str).str.contains(search, case=False, na=False)]
    if sel_cats:
        df2 = df2[df2["category"].isin(sel_cats)]
    if sel_tiers:
        df2 = df2[df2["performance_tier"].isin(sel_tiers)]
    df2 = df2[(df2["upload_hour"] >= hr[0]) & (df2["upload_hour"] <= hr[1])]
    df2 = df2[df2["view_count"] >= min_views]

    cols = ["title", "channel_name", "category", "view_count", "engagement_rate", "performance_tier"]
    existing = [c for c in cols if c in df2.columns]

    st.markdown(f'<div class="res-badge">🎬 {len(df2)} videos</div>', unsafe_allow_html=True)
    st.dataframe(
        styled_explorer(df2[existing].sort_values("engagement_rate", ascending=False)),
        use_container_width=True, hide_index=True,
    )


def page_deep_dive(df: pd.DataFrame) -> None:
    """Category Deep Dive page — per-category metrics, distribution, best time."""

    if df.empty:
        st.info("No data available.")
        return

    categories = sorted(df["category"].dropna().unique().tolist())
    if not categories:
        st.warning("No category data available.")
        return

    icons = {"Tech": "💻", "Finance": "💰", "Gaming": "🎮", "Fitness": "💪", "Education": "📚", "Comedy": "😂"}
    cat = st.radio(
        "Select category", categories, horizontal=True,
        format_func=lambda c: f"{icons.get(c, '📌')} {c}",
    )

    color = PALETTE[categories.index(cat) % len(PALETTE)]
    df2 = df[df["category"] == cat].copy()
    if df2.empty:
        st.warning("No data for this category.")
        return

    n = len(df2)
    ae = float(df2["engagement_rate"].mean())
    av = float(df2["view_count"].mean())
    vp = float((df2["performance_tier"] == "Viral").mean()) * 100 if "performance_tier" in df2.columns else 0.0

    c1, c2, c3, c4 = st.columns(4, gap="medium")
    with c1:
        metric_card("📹", "Videos", f"{n}", "v")
    with c2:
        metric_card("⚡", "Avg Engagement", f"{ae:.4f}%", "c")
    with c3:
        metric_card("👁️", "Avg Views", f"{av:,.0f}", "a")
    with c4:
        metric_card("🚀", "Viral %", f"{vp:.1f}%", "e")

    section_header(f"📉 Engagement Distribution — {cat}")
    fig = px.box(
        df2, x="category", y="engagement_rate",
        title=f"Engagement Rate Distribution — {cat}",
        points="outliers", color="category",
        color_discrete_sequence=[color],
    )
    chart_theme(fig)
    fig.update_layout(showlegend=False)
    show_chart(fig)

    section_header("🏆 Top 5 Videos")
    top5 = df2.sort_values("engagement_rate", ascending=False).head(5)[
        ["title", "channel_name", "view_count", "engagement_rate", "performance_tier"]
    ]
    st.dataframe(styled_explorer(top5), use_container_width=True, hide_index=True)

    section_header("⏰ Best Upload Slot")
    best = (
        df2.groupby(["upload_day", "upload_hour"], as_index=False)
        .agg(avg_engagement=("engagement_rate", "mean"))
        .sort_values("avg_engagement", ascending=False)
        .head(1)
    )
    if not best.empty:
        bd = best["upload_day"].iloc[0]
        bh = int(best["upload_hour"].iloc[0])
        bv = float(best["avg_engagement"].iloc[0])
        st.markdown(
            f"""
            <div class="best-box">
                <div class="best-box-label">● Optimal Time for {cat}</div>
                <div class="best-box-value">{bd} · {bh}:00</div>
                <div class="best-box-sub">Avg engagement: {bv:.4f}%</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


# ─── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    """Streamlit entry point."""

    # ── Hero Banner ──
    st.markdown(
        f"""
        <div class="hero">
            <h1 class="hero-title">{_LOGO_HERO} Creator Intelligence Platform</h1>
            <p class="hero-sub">Real-time YouTube analytics engine — track engagement, timing, and creator performance.</p>
            <div class="hero-badge"><div class="hero-dot"></div>Live Data</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Load Data ──
    with st.spinner("Loading data…"):
        df = load_videos_cleaned()
        last_updated = load_last_updated()

    # Coerce numeric columns
    if not df.empty:
        numeric_cols = [
            "engagement_rate", "view_count", "like_count", "comment_count",
            "duration_seconds", "subscriber_count", "upload_hour", "tag_count",
            "title_length", "title_word_count", "like_to_view_ratio",
            "comment_to_view_ratio", "total_channel_views", "total_video_count",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if "upload_hour" in df.columns:
            df["upload_hour"] = df["upload_hour"].fillna(0).astype(int)
        if "upload_day" in df.columns:
            df["upload_day"] = df["upload_day"].astype(str)

    # ── Sidebar ──
    st.sidebar.markdown(f'<div class="sb-logo">{_LOGO_HTML}</div>', unsafe_allow_html=True)
    st.sidebar.markdown('<div class="sb-name">Creator Intelligence</div>', unsafe_allow_html=True)

    n_vids = len(df) if isinstance(df, pd.DataFrame) else 0
    n_cats = df["category"].nunique() if isinstance(df, pd.DataFrame) and "category" in df.columns else 0
    st.sidebar.markdown(
        f'<div class="sb-stats">{n_vids:,} videos · {n_cats} categories</div>',
        unsafe_allow_html=True,
    )
    st.sidebar.markdown('<hr class="sb-divider">', unsafe_allow_html=True)

    if last_updated is not None:
        st.sidebar.caption(f"Last updated: {last_updated}")
    else:
        st.sidebar.caption("Last updated: —")

    st.sidebar.markdown('<div class="sb-section">Navigation</div>', unsafe_allow_html=True)

    page = st.sidebar.radio(
        "Navigate",
        options=[
            "📊 Overview",
            "⏰ Timing Intelligence",
            "🏅 Creator Leaderboard",
            "🔎 Video Explorer",
            "🧠 Category Deep Dive",
        ],
        index=0,
        label_visibility="collapsed",
    )

    # ── Page Title ──
    page_titles = {
        "📊 Overview": "Overview",
        "⏰ Timing Intelligence": "Timing Intelligence",
        "🏅 Creator Leaderboard": "Creator Leaderboard",
        "🔎 Video Explorer": "Video Explorer",
        "🧠 Category Deep Dive": "Category Deep Dive",
    }
    st.header(page_titles.get(page, ""))

    # ── Route ──
    if page == "📊 Overview":
        page_overview(df)
    elif page == "⏰ Timing Intelligence":
        page_timing(df)
    elif page == "🏅 Creator Leaderboard":
        page_leaderboard(df)
    elif page == "🔎 Video Explorer":
        page_explorer(df)
    elif page == "🧠 Category Deep Dive":
        page_deep_dive(df)

    # ── Sidebar Footer ──
    st.sidebar.markdown(
        """
        <div class="sb-foot">
            <div><span class="sb-badge">Python</span> <span class="sb-badge">MySQL</span> <span class="sb-badge">Streamlit</span></div>
            <div class="sb-powered">⚡ Powered by YouTube API v3</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Footer ──
    st.markdown(
        """
        <div class="app-footer">
            <span>🎯 <b>Creator Intelligence Platform</b></span>
            <span>Python · MySQL · Streamlit · Plotly</span>
            <span>© 2026 · Real YouTube Data</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
