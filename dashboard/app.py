"""
Creator Intelligence Platform - Streamlit dashboard.

Multi-page dashboard with sidebar navigation and Plotly visualizations.
Data is loaded directly from MySQL (`videos_cleaned`) via `database/db_connection.py`.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st
import plotly.express as px

from datetime import datetime
from typing import Optional

from database.db_connection import get_mysql_connection


st.set_page_config(
    page_title="Creator Intelligence Platform",
    layout="wide",
    page_icon="🎯",
)

# Premium palette
PRIMARY = "#7C3AED"
SECONDARY = "#06B6D4"
ACCENT = "#F59E0B"
SUCCESS = "#10B981"
DANGER = "#EF4444"
TEXT = "#F1F5F9"
BG_MAIN = "#0F1117"
BG_SIDEBAR = "#1A1D2E"
CARD_BG = "#1E2139"
GRID = "#2D3154"

PALETTE = [PRIMARY, SECONDARY, ACCENT, SUCCESS, DANGER, "#8B5CF6"]
CHART_HEIGHT = 450

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap');
    
    * {
        font-family: 'Inter', sans-serif !important;
    }
    
    .stApp {
        color: #F1F5F9;
        background: #0F1117;
    }
    [data-testid="stHeader"] {
        background: transparent !important;
    }
    [data-testid="stAppViewContainer"] .main .block-container {
        background: rgba(255, 255, 255, 0.02);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.05);
        border-radius: 16px;
        padding: 1rem 1.25rem 1.2rem 1.25rem;
    }
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1A1D2E 0%, #131629 100%);
    }
    .header-banner {
        background: linear-gradient(120deg, #7C3AED, #06B6D4, #7C3AED);
        background-size: 200% 200%;
        animation: gradientShift 8s ease infinite;
        border-radius: 16px;
        padding: 1.1rem 1.35rem;
        box-shadow: 0 12px 26px rgba(124, 58, 237, 0.2);
        margin-bottom: 1rem;
        border-bottom: 1px solid rgba(241, 245, 249, 0.35);
        position: relative;
    }
    .header-banner:after {
        content: "";
        position: absolute;
        left: 0;
        right: 0;
        bottom: 0;
        height: 2px;
        box-shadow: 0 0 12px rgba(6, 182, 212, 0.95);
    }
    .header-title {
        margin: 0;
        background: linear-gradient(135deg, #FFFFFF 0%, #E2E8F0 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 34px;
        font-weight: 800;
        letter-spacing: 0.2px;
    }
    .header-subtitle {
        margin-top: 0.25rem;
        color: rgba(241, 245, 249, 0.92);
        font-size: 16px;
    }
    .live-indicator {
        margin-top: 0.45rem;
        color: #f8fbff;
        font-size: 0.88rem;
        font-weight: 700;
        letter-spacing: 0.4px;
    }
    .live-dot {
        color: #10B981;
        animation: pulseLive 1.5s ease infinite;
    }
    @keyframes pulseLive {
        0% { opacity: 1; text-shadow: 0 0 0 rgba(16, 185, 129, 0.7); }
        50% { opacity: 0.55; text-shadow: 0 0 8px rgba(16, 185, 129, 0.85); }
        100% { opacity: 1; text-shadow: 0 0 0 rgba(16, 185, 129, 0.7); }
    }
    @keyframes gradientShift {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }
    .cip-logo-box {
        border: 1px solid rgba(124, 58, 237, 0.78);
        background: rgba(124, 58, 237, 0.2);
        border-radius: 14px;
        padding: 0.9rem;
        text-align: center;
        font-size: 2rem;
        font-weight: 800;
        color: #ffffff;
        box-shadow: 0 0 16px rgba(124, 58, 237, 0.42);
        margin-bottom: 0.65rem;
    }
    .sidebar-stats {
        font-size: 0.88rem;
        color: #c9d4ef;
        text-align: center;
        margin-bottom: 0.55rem;
    }
    .sidebar-divider {
        border: none;
        border-top: 1px solid rgba(241, 245, 249, 0.2);
        margin: 0.55rem 0 0.8rem 0;
    }
    .section-header {
        margin: 0.45rem 0 0.75rem 0;
        color: #f4f7ff;
        font-size: 1.2rem;
        font-weight: 700;
        display: inline-block;
        padding-bottom: 0.2rem;
        border-bottom: 2px solid transparent;
        border-image: linear-gradient(90deg, #7C3AED 0%, #06B6D4 100%);
        border-image-slice: 1;
    }
    .dot {
        color: #7C3AED;
        margin-right: 0.38rem;
        font-size: 1.08rem;
    }
    .metric-card {
        background: rgba(30, 33, 57, 0.7);
        backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 14px;
        padding: 20px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.2);
        min-height: 126px;
        transition: transform 0.3s cubic-bezier(0.4, 0, 0.2, 1), box-shadow 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    }
    .metric-card:hover {
        transform: translateY(-5px) scale(1.02);
        box-shadow: 0 16px 32px rgba(124, 58, 237, 0.25);
        border-color: rgba(255, 255, 255, 0.15);
    }
    .metric-card-1 { background: linear-gradient(145deg, #7C3AED 0%, #4F46E5 100%); }
    .metric-card-2 { background: linear-gradient(145deg, #06B6D4 0%, #3B82F6 100%); }
    .metric-card-3 { background: linear-gradient(145deg, #F59E0B 0%, #F97316 100%); }
    .metric-card-4 { background: linear-gradient(145deg, #10B981 0%, #14B8A6 100%); }
    .metric-icon {
        font-size: 2rem;
        display: block;
        margin-bottom: 12px;
        opacity: 0.9;
        font-weight: bold;
        color: #FFFFFF;
    }
    .metric-label {
        color: rgba(241, 245, 249, 0.95);
        font-size: 0.83rem;
        margin-top: 0.55rem;
        margin-bottom: 0;
    }
    .metric-value {
        color: #FFFFFF;
        font-size: 1.85rem;
        font-weight: 800;
        line-height: 1.15;
        text-align: center;
    }
    .chart-card {
        background: #1E2139;
        border-radius: 14px;
        padding: 0.35rem 0.45rem 0.2rem 0.45rem;
        margin-bottom: 0.6rem;
    }
    [data-testid="stPlotlyChart"] {
        background: transparent;
        border: 0;
    }
    .summary-banner {
        background: rgba(124, 58, 237, 0.17);
        border: 1px solid rgba(124, 58, 237, 0.5);
        border-radius: 14px;
        padding: 0.7rem 0.85rem;
        margin: 0.4rem 0 0.85rem 0;
    }
    .insight-card {
        background: rgba(25, 30, 50, 0.95);
        border: 1px solid rgba(124, 58, 237, 0.3);
        border-left: 4px solid #06B6D4;
        border-radius: 10px;
        padding: 0.65rem 0.75rem;
        margin-bottom: 0.5rem;
        transition: transform 0.16s ease, border-color 0.16s ease;
    }
    .insight-card:hover {
        transform: translateX(2px);
        border-color: rgba(124, 58, 237, 0.7);
    }
    .best-time-box {
        background: rgba(16, 185, 129, 0.22);
        border: 1px solid rgba(16, 185, 129, 0.92);
        box-shadow: 0 0 14px rgba(16, 185, 129, 0.26);
        color: #ffffff;
        border-radius: 10px;
        padding: 0.8rem;
        margin-bottom: 0.75rem;
        font-weight: 600;
    }
    .result-badge {
        display: inline-block;
        background: rgba(124, 58, 237, 0.22);
        border: 1px solid rgba(124, 58, 237, 0.6);
        color: #e9ddff;
        border-radius: 999px;
        padding: 0.25rem 0.65rem;
        font-size: 0.86rem;
        margin-bottom: 0.45rem;
    }
    .tier-viral { background: rgba(124,58,237,0.25); color: #E9DDFF; border: 1px solid rgba(124,58,237,0.6); border-radius: 8px; padding: 2px 8px; }
    .tier-high { background: rgba(59,130,246,0.23); color: #D9E8FF; border: 1px solid rgba(59,130,246,0.6); border-radius: 8px; padding: 2px 8px; }
    .tier-medium { background: rgba(245,158,11,0.24); color: #FFF0C9; border: 1px solid rgba(245,158,11,0.7); border-radius: 8px; padding: 2px 8px; }
    .tier-low { background: rgba(148,163,184,0.2); color: #E2E8F0; border: 1px solid rgba(148,163,184,0.5); border-radius: 8px; padding: 2px 8px; }
    .filter-pill-caption {
        font-size: 0.8rem;
        color: #aeb9d8;
        margin-bottom: -0.25rem;
    }
    div[role="radiogroup"] > label {
        background: rgba(124, 58, 237, 0.15);
        border: 1px solid rgba(124, 58, 237, 0.45);
        border-radius: 999px;
        margin-bottom: 0.4rem;
        padding: 0.36rem 0.52rem;
        transition: all 0.24s cubic-bezier(0.22, 1, 0.36, 1);
    }
    div[role="radiogroup"] > label:hover {
        background: rgba(124, 58, 237, 0.3);
        border-color: rgba(6, 182, 212, 0.8);
        transform: translateX(2px);
    }
    div[role="radiogroup"] > label:has(input:checked) {
        background: rgba(124, 58, 237, 0.45);
        border-color: rgba(124, 58, 237, 0.95);
        box-shadow: 0 0 0 1px rgba(124, 58, 237, 0.5) inset;
    }
    button[data-baseweb="tab"] {
        border-radius: 999px !important;
        border: 1px solid rgba(124, 58, 237, 0.4) !important;
        background: rgba(124, 58, 237, 0.1) !important;
        color: #dbe4ff !important;
        transition: all 0.2s ease !important;
    }
    button[data-baseweb="tab"]:hover {
        background: rgba(124, 58, 237, 0.24) !important;
        border-color: rgba(6, 182, 212, 0.7) !important;
    }
    button[data-baseweb="tab"][aria-selected="true"] {
        background: rgba(124, 58, 237, 0.42) !important;
        border-color: rgba(124, 58, 237, 0.95) !important;
        box-shadow: 0 0 0 1px rgba(124, 58, 237, 0.45) inset !important;
        color: #ffffff !important;
    }
    .sidebar-powered {
        font-size: 0.82rem;
        color: #afbbdf;
        margin-top: 1rem;
        text-align: center;
    }
    .tech-badges {
        margin-top: 0.6rem;
        text-align: center;
    }
    .tech-badge {
        display: inline-block;
        background: rgba(124, 58, 237, 0.2);
        border: 1px solid rgba(124, 58, 237, 0.55);
        color: #ece8ff;
        border-radius: 999px;
        padding: 0.2rem 0.5rem;
        margin: 0.13rem;
        font-size: 0.74rem;
    }
    .footer {
        width: 100%;
        background: #1A1D2E;
        border-radius: 8px;
        border-top: 1px solid #7C3AED;
        color: #94A3B8;
        margin-top: 1rem;
        padding: 0.7rem 0.85rem;
        font-size: 0.82rem;
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 0.65rem;
    }
    @media (max-width: 1200px) {
        [data-testid="stAppViewContainer"] .main .block-container {
            padding: 0.85rem 0.9rem 1rem 0.9rem;
        }
        .header-title { font-size: 28px; }
        .header-subtitle { font-size: 15px; }
        .metric-card {
            min-height: 108px;
            padding: 15px;
        }
        .metric-value { font-size: 1.35rem; }
    }
    @media (max-width: 768px) {
        [data-testid="stAppViewContainer"] .main .block-container {
            padding: 0.7rem 0.6rem 0.85rem 0.6rem;
        }
        .header-banner {
            padding: 0.85rem 0.9rem;
            border-radius: 12px;
        }
        .header-title { font-size: 24px; }
        .header-subtitle { font-size: 14px; }
        .metric-card {
            min-height: 96px;
            padding: 12px;
        }
        .metric-value { font-size: 1.18rem; }
        .section-header {
            font-size: 1.06rem;
            margin-top: 0.3rem;
        }
        div[role="radiogroup"] > label {
            margin-bottom: 0.28rem;
            padding: 0.28rem 0.42rem;
        }
        .footer { display:block; text-align:center; }
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def section_header(text: str) -> None:
    """
    Render a styled section header with a colored dot + gradient underline.
    """

    st.markdown(
        f'<div class="section-header"><span class="dot">●</span>{text}</div>',
        unsafe_allow_html=True,
    )


def render_metric_card(icon: str, label: str, value: str, css_class: str) -> None:
    """
    Render a custom metric card using HTML/CSS.
    """

    st.markdown(
        f"""
        <div class="metric-card {css_class}">
            <div class="metric-icon">{icon}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-label">{label}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def style_dataframe(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    """
    Return a dark-themed styled DataFrame with alternating row colors.
    """

    return (
        df.style.set_properties(
            **{
                "background-color": CARD_BG,
                "color": TEXT,
                "border": "1px solid #2D3154",
            }
        )
        .set_table_styles(
            [
                {"selector": "th", "props": [("background-color", "#252A45"), ("color", TEXT)]},
                {"selector": "td", "props": [("font-size", "0.86rem")]},
            ]
        )
        .apply(
            lambda row: [
                "background-color: #1A1D2E" if row.name % 2 == 0 else "background-color: #1E2139"
                for _ in row
            ],
            axis=1,
        )
    )


def style_leaderboard_dataframe(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    """
    Styled leaderboard table with alternating rows and highlighted rank badges.
    """

    styled = style_dataframe(df)
    if "rank" in df.columns:
        styled = styled.apply(
            lambda col: [
                "background-color:#7C3AED;color:#fff;font-weight:700;border-radius:6px;" if isinstance(v, str) and ("🥇" in v or "#1" in v)
                else "background-color:#06B6D4;color:#fff;font-weight:700;border-radius:6px;" if isinstance(v, str) and ("🥈" in v or "#2" in v)
                else "background-color:#F59E0B;color:#1f2937;font-weight:700;border-radius:6px;" if isinstance(v, str) and ("🥉" in v or "#3" in v)
                else "background-color:#252A45;color:#E2E8F0;"
                for v in col
            ],
            subset=["rank"],
        )
    return styled


def style_video_explorer_dataframe(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    """
    Styled explorer table with color-coded performance tier badges.
    """

    styled = style_dataframe(df)
    if "performance_tier" in df.columns:
        styled = styled.apply(
            lambda col: [
                "background-color:rgba(124,58,237,0.25);color:#E9DDFF;font-weight:700;" if str(v) == "Viral"
                else "background-color:rgba(59,130,246,0.23);color:#D9E8FF;font-weight:700;" if str(v) == "High"
                else "background-color:rgba(245,158,11,0.24);color:#FFF0C9;font-weight:700;" if str(v) == "Medium"
                else "background-color:rgba(148,163,184,0.2);color:#E2E8F0;font-weight:700;" if str(v) == "Low"
                else ""
                for v in col
            ],
            subset=["performance_tier"],
        )
    return styled


def apply_chart_theme(fig):
    """
    Apply a consistent dark premium chart theme.
    """

    fig.update_layout(
        template="plotly_dark",
        height=CHART_HEIGHT,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor=CARD_BG,
        font_color=TEXT,
        font=dict(color=TEXT),
        hoverlabel=dict(bgcolor="#252A45", font_color=TEXT),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)", zerolinecolor="rgba(255,255,255,0.05)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)", zerolinecolor="rgba(255,255,255,0.05)"),
        transition=dict(duration=600, easing="cubic-in-out"),
    )
    fig.update_xaxes(showline=False, mirror=False, zeroline=False)
    fig.update_yaxes(showline=False, mirror=False, zeroline=False)
    return fig


def render_chart(fig) -> None:
    """
    Render Plotly chart with hidden toolbar.
    """

    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    st.markdown('</div>', unsafe_allow_html=True)


@st.cache_data
def load_videos_cleaned() -> pd.DataFrame:
    """
    Load cleaned dataset from MySQL (`videos_cleaned`) into a DataFrame.
    """

    conn = get_mysql_connection()
    try:
        return pd.read_sql("SELECT * FROM videos_cleaned", conn)
    finally:
        conn.close()


@st.cache_data
def load_last_updated() -> Optional[datetime]:
    """
    Fetch the latest `collected_at` timestamp from `videos_cleaned`.
    """

    conn = get_mysql_connection()
    try:
        df = pd.read_sql(
            "SELECT MAX(collected_at) AS last_updated FROM videos_cleaned",
            conn,
        )
        val = df["last_updated"].iloc[0] if not df.empty else None
        return val
    finally:
        conn.close()


def safe_numeric_series(series: pd.Series) -> pd.Series:
    """
    Convert a series to numeric dtype, coercing invalid values to NaN.
    """

    return pd.to_numeric(series, errors="coerce")


def render_overview(df: pd.DataFrame) -> None:
    """
    Render the Overview page.
    """

    st.header("📊 Overview")
    if df.empty:
        st.info("No cleaned data found. Collect and run cleaning first.")
        return

    st.markdown('<div class="summary-banner">📊 <b>Platform Summary</b> — Performance overview across videos, engagement, and category momentum.</div>', unsafe_allow_html=True)

    total_videos = int(len(df))
    avg_engagement = float(safe_numeric_series(df["engagement_rate"]).mean())
    total_views = int(safe_numeric_series(df["view_count"]).sum())
    most_active_category = (
        df["category"].value_counts().idxmax() if "category" in df.columns and not df.empty else "N/A"
    )

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        render_metric_card("▶", "Total Videos", f"{total_videos}", "metric-card-1")
    with c2:
        render_metric_card("◎", "Avg Engagement Rate", f"{avg_engagement:.4f}%", "metric-card-2")
    with c3:
        render_metric_card("◉", "Total Views", f"{total_views:,}", "metric-card-3")
    with c4:
        render_metric_card("★", "Most Active Category", str(most_active_category), "metric-card-4")

    category_counts = df["category"].value_counts().reset_index()
    category_counts.columns = ["category", "video_count"]

    category_engagement = (
        df.groupby("category", as_index=False)
        .agg(avg_engagement_rate=("engagement_rate", "mean"))
        .sort_values("avg_engagement_rate", ascending=False)
    )

    tier_counts = df["performance_tier"].value_counts().reset_index()
    tier_counts.columns = ["performance_tier", "video_count"]

    left, right = st.columns((1.25, 1))
    with left:
        section_header("📦 Video Count by Category")
        fig_count = px.bar(
            category_counts,
            x="category",
            y="video_count",
            title="Video Count by Category",
            color="category",
            color_discrete_sequence=PALETTE,
        )
        apply_chart_theme(fig_count)
        render_chart(fig_count)
    with right:
        section_header("⭐ Performance Tier Distribution")
        fig_pie = px.pie(
            tier_counts,
            names="performance_tier",
            values="video_count",
            title="Performance Tier Distribution",
            hole=0.42,
            color="performance_tier",
            color_discrete_sequence=PALETTE,
        )
        apply_chart_theme(fig_pie)
        render_chart(fig_pie)

    section_header("💬 Average Engagement Rate by Category")
    fig_eng = px.bar(
        category_engagement,
        x="category",
        y="avg_engagement_rate",
        title="Average Engagement Rate by Category",
        color="category",
        color_discrete_sequence=PALETTE,
    )
    apply_chart_theme(fig_eng)
    render_chart(fig_eng)

    section_header("🔥 Top 3 Insights")
    top_category = category_engagement.head(1)
    best_category = top_category["category"].iloc[0] if not top_category.empty else "N/A"
    best_eng = float(top_category["avg_engagement_rate"].iloc[0]) if not top_category.empty else 0.0
    best_hour_df = (
        df.groupby("upload_hour", as_index=False)
        .agg(avg_views=("view_count", "mean"))
        .sort_values("avg_views", ascending=False)
        .head(1)
    )
    best_hour = int(best_hour_df["upload_hour"].iloc[0]) if not best_hour_df.empty else 0
    industry_engagement_benchmark = 4.50
    vs_industry = avg_engagement - industry_engagement_benchmark
    comparison = "above" if vs_industry >= 0 else "below"
    st.markdown(
        f"""
        <div class="insight-card">📌 Best performing category: <b>{best_category}</b> at <b>{best_eng:.4f}%</b> engagement.</div>
        <div class="insight-card">⏰ Optimal upload hour: <b>{best_hour}:00</b> based on highest average views.</div>
        <div class="insight-card">📊 Avg engagement is <b>{abs(vs_industry):.2f}%</b> {comparison} the reference benchmark of <b>{industry_engagement_benchmark:.2f}%</b>.</div>
        """,
        unsafe_allow_html=True,
    )


def render_timing_intelligence(df: pd.DataFrame) -> None:
    """
    Render the Timing Intelligence page.
    """

    st.header("⏰ Timing Intelligence")
    if df.empty:
        st.info("No cleaned data found. Collect and run cleaning first.")
        return

    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    df2 = df.copy()
    df2["upload_day"] = df2["upload_day"].astype(str)

    heatmap_data = (
        df2.groupby(["upload_day", "upload_hour"], as_index=False)
        .agg(avg_engagement=("engagement_rate", "mean"))
    )

    # Create a complete matrix for imshow.
    pivot = (
        heatmap_data.pivot(index="upload_day", columns="upload_hour", values="avg_engagement")
        .reindex(day_order)
    )

    best = (
        df2.groupby(["upload_day", "upload_hour"], as_index=False)
        .agg(avg_engagement=("engagement_rate", "mean"))
        .sort_values("avg_engagement", ascending=False)
        .head(1)
    )
    if not best.empty:
        best_day = best["upload_day"].iloc[0]
        best_hour = int(best["upload_hour"].iloc[0])
        best_val = float(best["avg_engagement"].iloc[0])
        st.markdown(
            f'<div class="best-time-box"><div style="font-size:0.78rem;letter-spacing:0.6px;">BEST TIME TO POST</div><div style="font-size:1.5rem;font-weight:800;">{best_day} • {best_hour}:00</div><div style="font-size:0.88rem;">Avg engagement: {best_val:.4f}%</div></div>',
            unsafe_allow_html=True,
        )

    section_header("🧭 Best Upload Hour vs Day")
    fig_heat = px.imshow(
        pivot,
        aspect="auto",
        title="Best Upload Hour vs Day (Avg Engagement Rate)",
        labels={"x": "Upload Hour", "y": "Upload Day", "color": "Avg Engagement Rate"},
        color_continuous_scale=[PRIMARY, SECONDARY, ACCENT],
    )
    apply_chart_theme(fig_heat)
    render_chart(fig_heat)

    avg_views_by_hour = (
        df2.groupby("upload_hour", as_index=False)
        .agg(avg_views=("view_count", "mean"))
        .sort_values("upload_hour")
    )

    section_header("📈 Avg Views by Upload Hour")
    fig_line = px.line(
        avg_views_by_hour,
        x="upload_hour",
        y="avg_views",
        title="Avg Views by Upload Hour",
        markers=True,
    )
    fig_line.update_traces(line=dict(color=SECONDARY, width=3), marker=dict(color=PRIMARY, size=8))
    apply_chart_theme(fig_line)
    render_chart(fig_line)

    section_header("📅 Day-of-Week Performance Ranking")
    day_rank = (
        df2.groupby("upload_day", as_index=False)
        .agg(avg_engagement=("engagement_rate", "mean"), avg_views=("view_count", "mean"))
        .sort_values("avg_engagement", ascending=False)
        .reset_index(drop=True)
    )
    day_rank.insert(0, "rank", [f"#{i+1}" for i in range(len(day_rank))])
    st.dataframe(style_dataframe(day_rank), use_container_width=True, hide_index=True)


def render_creator_leaderboard(df: pd.DataFrame) -> None:
    """
    Render the Creator Leaderboard page.
    """

    st.header("🏅 Creator Leaderboard")
    if df.empty:
        st.info("No cleaned data found. Collect and run cleaning first.")
        return

    categories = sorted(df["category"].dropna().unique().tolist())
    tier = st.selectbox("🎯 Filter by category", options=["All"] + categories, index=0)

    if tier == "All":
        df2 = df
    else:
        df2 = df[df["category"] == tier]

    channel_leaders = (
        df2.groupby(["channel_id", "channel_name"], as_index=False)
        .agg(
            avg_engagement_rate=("engagement_rate", "mean"),
            total_views=("view_count", "sum"),
            video_count=("video_id", "count") if "video_id" in df2.columns else ("view_count", "size"),
        )
        .sort_values("avg_engagement_rate", ascending=False)
        .head(20)
    )

    section_header("📋 Top Channels by Avg Engagement")
    display_table = channel_leaders.copy()
    medals = ["🥇", "🥈", "🥉"]
    rank_badges = []
    for idx in range(len(display_table)):
        rank_badges.append(f"{medals[idx]} #{idx + 1}" if idx < 3 else f"#{idx + 1}")
    display_table.insert(0, "rank", rank_badges)
    st.dataframe(style_leaderboard_dataframe(display_table), use_container_width=True, hide_index=True)

    top_views = (
        df2.groupby(["channel_id", "channel_name"], as_index=False)
        .agg(total_views=("view_count", "sum"))
        .sort_values("total_views", ascending=False)
        .head(10)
    )

    section_header("📊 Top 10 Channels by Total Views")
    fig_views = px.bar(
        top_views.sort_values("total_views", ascending=True),
        x="total_views",
        y="channel_name",
        orientation="h",
        title="Top 10 Channels by Total Views",
        color="channel_name",
        color_discrete_sequence=PALETTE,
    )
    apply_chart_theme(fig_views)
    fig_views.update_layout(showlegend=False)
    render_chart(fig_views)

    section_header("✨ Rising Stars")
    rising = channel_leaders[channel_leaders["video_count"] >= 3].sort_values(
        ["avg_engagement_rate", "video_count"], ascending=[False, False]
    ).head(5)
    st.dataframe(style_dataframe(rising), use_container_width=True, hide_index=True)


def render_video_explorer(df: pd.DataFrame) -> None:
    """
    Render the Video Explorer page.
    """

    st.header("🔎 Video Explorer")
    if df.empty:
        st.info("No cleaned data found. Collect and run cleaning first.")
        return

    categories = sorted(df["category"].dropna().unique().tolist())
    tiers = ["Viral", "High", "Medium", "Low"]

    st.markdown('<div class="filter-pill-caption">Filter pills</div>', unsafe_allow_html=True)
    search_text = st.text_input("🔤 Search title")
    selected_categories = st.multiselect("🗂️ Category", options=categories, default=categories)
    selected_tiers = st.multiselect("🚦 Performance Tier", options=tiers, default=tiers)

    min_hour, max_hour = 0, 23
    hour_range = st.slider("🕒 Upload hour range", min_hour, max_hour, (min_hour, max_hour))

    max_views_val = pd.to_numeric(df["view_count"], errors="coerce").max() if not df.empty else 0
    max_views = int(max_views_val) if pd.notna(max_views_val) else 0
    min_views = st.slider("👁️ Minimum views", 0, max_views if max_views > 0 else 1, 0)

    df2 = df.copy()
    if search_text:
        df2 = df2[df2["title"].astype(str).str.contains(search_text, case=False, na=False)]

    if selected_categories:
        df2 = df2[df2["category"].isin(selected_categories)]
    if selected_tiers:
        df2 = df2[df2["performance_tier"].isin(selected_tiers)]

    df2 = df2[(df2["upload_hour"] >= hour_range[0]) & (df2["upload_hour"] <= hour_range[1])]
    df2 = df2[df2["view_count"] >= min_views]

    show_cols = [
        "title",
        "channel_name",
        "category",
        "view_count",
        "engagement_rate",
        "performance_tier",
    ]
    existing_cols = [c for c in show_cols if c in df2.columns]

    st.markdown(f'<div class="result-badge">Results: {len(df2)} videos</div>', unsafe_allow_html=True)
    st.dataframe(
        style_video_explorer_dataframe(df2[existing_cols].sort_values("engagement_rate", ascending=False)),
        use_container_width=True,
        hide_index=True,
    )


def render_category_deep_dive(df: pd.DataFrame) -> None:
    """
    Render the Category Deep Dive page.
    """

    st.header("🧠 Category Deep Dive")
    if df.empty:
        st.info("No cleaned data found. Collect and run cleaning first.")
        return

    categories = sorted(df["category"].dropna().unique().tolist())
    if not categories:
        st.warning("No category data available.")
        return

    icon_map = {
        "Tech": "💻",
        "Finance": "💰",
        "Gaming": "🎮",
        "Fitness": "💪",
        "Education": "📚",
        "Comedy": "😂",
    }
    cat = st.radio(
        "Choose category",
        categories,
        horizontal=True,
        format_func=lambda c: f"{icon_map.get(c, '📌')} {c}",
    )

    color_for_category = PALETTE[categories.index(cat) % len(PALETTE)]
    df2 = df[df["category"] == cat].copy()
    if df2.empty:
        st.warning("No data for this category.")
        return

    total_videos = int(len(df2))
    avg_engagement = float(df2["engagement_rate"].mean())
    avg_views = float(df2["view_count"].mean())
    viral_pct = (
        float((df2["performance_tier"] == "Viral").mean()) * 100.0
        if "performance_tier" in df2.columns
        else 0.0
    )

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        render_metric_card("▶", "Total Videos", f"{total_videos}", "metric-card-1")
    with c2:
        render_metric_card("◎", "Avg Engagement", f"{avg_engagement:.4f}%", "metric-card-2")
    with c3:
        render_metric_card("◉", "Avg Views", f"{avg_views:,.0f}", "metric-card-3")
    with c4:
        render_metric_card("★", "Viral %", f"{viral_pct:.2f}%", "metric-card-4")

    section_header(f"📉 Engagement Distribution — {cat}")
    fig_box = px.box(
        df2,
        x="category",
        y="engagement_rate",
        title=f"Engagement Rate Distribution - {cat}",
        points="outliers",
        color="category",
        color_discrete_sequence=[color_for_category],
    )
    apply_chart_theme(fig_box)
    fig_box.update_layout(showlegend=False)
    render_chart(fig_box)

    section_header("🏆 Top 5 Videos in This Category")
    top5 = df2.sort_values("engagement_rate", ascending=False).head(5)[
        ["title", "channel_name", "view_count", "engagement_rate", "performance_tier"]
    ]
    st.dataframe(style_video_explorer_dataframe(top5), use_container_width=True, hide_index=True)

    section_header("🕒 Best Upload Time for This Category")
    best = (
        df2.groupby(["upload_day", "upload_hour"], as_index=False)
        .agg(avg_engagement=("engagement_rate", "mean"))
        .sort_values("avg_engagement", ascending=False)
        .head(1)
    )
    if not best.empty:
        best_day = best["upload_day"].iloc[0]
        best_hour = int(best["upload_hour"].iloc[0])
        best_val = float(best["avg_engagement"].iloc[0])
        st.success(f"Best time to post is {best_day} at {best_hour}:00 (avg engagement {best_val:.4f}%)")


def main() -> None:
    """
    Streamlit entrypoint.
    """

    st.markdown(
        """
        <div class="header-banner">
            <h1 class="header-title">🎯 Creator Intelligence Platform</h1>
            <div class="header-subtitle">Real-time YouTube Analytics Engine</div>
            <div class="live-indicator"><span class="live-dot">●</span> LIVE DATA</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.spinner("🔍 Fetching creator data..."):
        df = load_videos_cleaned()
        last_updated = load_last_updated()

    with st.spinner("📊 Crunching the numbers..."):
        pass

    if not df.empty:
        # Coerce expected numeric fields for stable filtering/grouping even with small datasets.
        for col in [
            "engagement_rate",
            "view_count",
            "like_count",
            "comment_count",
            "duration_seconds",
            "subscriber_count",
            "upload_hour",
            "tag_count",
            "title_length",
            "title_word_count",
            "like_to_view_ratio",
            "comment_to_view_ratio",
            "total_channel_views",
            "total_video_count",
        ]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if "upload_hour" in df.columns:
            df["upload_hour"] = df["upload_hour"].fillna(0).astype(int)
        if "upload_day" in df.columns:
            df["upload_day"] = df["upload_day"].astype(str)

    st.sidebar.markdown('<div class="cip-logo-box">🎯</div>', unsafe_allow_html=True)
    st.sidebar.markdown('<div style="text-align:center;font-weight:800;color:#EEF2FF;margin-bottom:0.35rem;">Creator Intelligence</div>', unsafe_allow_html=True)
    total_vid_sidebar = len(df) if isinstance(df, pd.DataFrame) else 0
    cat_sidebar = df["category"].nunique() if isinstance(df, pd.DataFrame) and "category" in df.columns else 0
    st.sidebar.markdown(
        f'<div class="sidebar-stats">{total_vid_sidebar} Videos | {cat_sidebar} Categories | Live Data</div>',
        unsafe_allow_html=True,
    )
    st.sidebar.markdown('<hr class="sidebar-divider">', unsafe_allow_html=True)

    if last_updated is not None:
        st.sidebar.caption(f"Last updated: {last_updated}")
    else:
        st.sidebar.caption("Last updated: N/A")

    page = st.sidebar.radio(
        "🧭 Navigation",
        options=[
            "📊 Overview",
            "⏰ Timing Intelligence",
            "🏅 Creator Leaderboard",
            "🔎 Video Explorer",
            "🧠 Category Deep Dive",
        ],
        index=0,
    )

    if page == "📊 Overview":
        render_overview(df)
    elif page == "⏰ Timing Intelligence":
        render_timing_intelligence(df)
    elif page == "🏅 Creator Leaderboard":
        render_creator_leaderboard(df)
    elif page == "🔎 Video Explorer":
        render_video_explorer(df)
    elif page == "🧠 Category Deep Dive":
        render_category_deep_dive(df)

    st.sidebar.markdown(
        '<div class="tech-badges"><span class="tech-badge">🐍 Python</span><span class="tech-badge">🗄️ MySQL</span><span class="tech-badge">📊 Streamlit</span></div>',
        unsafe_allow_html=True,
    )
    st.sidebar.markdown('<div class="sidebar-powered">⚡ Powered by YouTube API v3</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="footer"><span>🎯 Creator Intelligence Platform</span><span>Built with Python • SQL • MySQL • Streamlit</span><span>© 2026 | Real YouTube Data</span></div>',
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()

