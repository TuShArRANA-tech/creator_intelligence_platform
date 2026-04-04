"""
Creator Intelligence Platform - Power BI export utility.

Exports CSV files from MySQL to `powerbi/` for Power BI import:
- videos_cleaned.csv
- channels_summary.csv
- category_summary.csv
- timing_analysis.csv
- performance_tiers.csv
"""

from __future__ import annotations

import os
from typing import Optional

import pandas as pd

from database.db_connection import get_mysql_connection


def load_table_df(table_name: str) -> pd.DataFrame:
    """
    Load an entire MySQL table into a pandas DataFrame.

    Args:
        table_name: MySQL table name.

    Returns:
        pd.DataFrame: Table data.
    """

    conn = get_mysql_connection()
    try:
        return pd.read_sql(f"SELECT * FROM {table_name}", conn)
    finally:
        conn.close()


def export_csv(df: pd.DataFrame, out_path: str) -> None:
    """
    Export DataFrame to CSV with UTF-8 encoding.
    """

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")


def export_for_powerbi() -> None:
    """
    Build and export all Power BI CSV files.
    """

    print("Loading `videos_cleaned` from MySQL...")
    df = load_table_df("videos_cleaned")
    if df.empty:
        print("[WARN] `videos_cleaned` is empty. Exports will be mostly empty CSVs.")

    powerbi_dir = "powerbi"
    videos_cleaned_path = os.path.join(powerbi_dir, "videos_cleaned.csv")
    export_csv(df, videos_cleaned_path)

    # Channels summary (aggregated metrics per channel).
    print("Building `channels_summary.csv`...")
    channels_summary = (
        df.groupby(["channel_id", "channel_name", "subscriber_count", "total_video_count", "total_channel_views", "country"], as_index=False)
        .agg(
            total_views=("view_count", "sum"),
            avg_views=("view_count", "mean"),
            avg_engagement_rate=("engagement_rate", "mean"),
            videos_count=("video_id", "count") if "video_id" in df.columns else ("view_count", "size"),
        )
        .sort_values("avg_engagement_rate", ascending=False)
    )
    export_csv(channels_summary, os.path.join(powerbi_dir, "channels_summary.csv"))

    # Category summary (KPIs per category).
    print("Building `category_summary.csv`...")
    total_views_all = float(df["view_count"].sum()) if not df.empty else 0.0
    category_summary = (
        df.groupby("category", as_index=False)
        .agg(
            total_videos=("video_id", "count") if "video_id" in df.columns else ("view_count", "size"),
            total_views=("view_count", "sum"),
            avg_views=("view_count", "mean"),
            avg_engagement_rate=("engagement_rate", "mean"),
            viral_videos=("performance_tier", lambda s: int((s == "Viral").sum())),
        )
    )
    if total_views_all > 0:
        category_summary["pct_of_total_views"] = (category_summary["total_views"] / total_views_all) * 100.0
    else:
        category_summary["pct_of_total_views"] = 0.0
    export_csv(category_summary, os.path.join(powerbi_dir, "category_summary.csv"))

    # Timing analysis (upload hour/day vs avg engagement).
    print("Building `timing_analysis.csv`...")
    timing_analysis = (
        df.groupby(["upload_day", "upload_hour"], as_index=False)
        .agg(
            videos_count=("video_id", "count") if "video_id" in df.columns else ("view_count", "size"),
            avg_engagement_rate=("engagement_rate", "mean"),
            avg_views=("view_count", "mean"),
        )
        .sort_values(["upload_day", "upload_hour"])
    )
    export_csv(timing_analysis, os.path.join(powerbi_dir, "timing_analysis.csv"))

    # Performance tiers distribution by category.
    print("Building `performance_tiers.csv`...")
    perf_tiers = (
        df.groupby(["category", "performance_tier"], as_index=False)
        .agg(videos_count=("video_id", "count") if "video_id" in df.columns else ("view_count", "size"))
    )
    totals = perf_tiers.groupby("category", as_index=False)["videos_count"].sum().rename(columns={"videos_count": "category_total_videos"})
    perf_tiers = perf_tiers.merge(totals, on="category", how="left")
    perf_tiers["pct_of_category"] = (perf_tiers["videos_count"] / perf_tiers["category_total_videos"]) * 100.0
    export_csv(perf_tiers, os.path.join(powerbi_dir, "performance_tiers.csv"))

    print("\nPower BI exports complete:")
    print(f"- {videos_cleaned_path}")
    print("- powerbi/channels_summary.csv")
    print("- powerbi/category_summary.csv")
    print("- powerbi/timing_analysis.csv")
    print("- powerbi/performance_tiers.csv")


if __name__ == "__main__":
    export_for_powerbi()

