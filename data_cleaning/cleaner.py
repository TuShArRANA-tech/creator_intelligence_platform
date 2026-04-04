"""
Creator Intelligence Platform - Data cleaning pipeline.

Loads raw `videos` joined with `channels` from MySQL, cleans and enriches the dataset,
then writes cleaned output to:
- MySQL table: `videos_cleaned`
- CSV for Power BI: `powerbi/cleaned_data.csv`
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Tuple

import pandas as pd
import numpy as np
from mysql.connector import MySQLConnection

from database.db_connection import get_mysql_connection


def load_raw_data(connection: MySQLConnection) -> pd.DataFrame:
    """
    Load raw video + channel data from MySQL into a pandas DataFrame.

    Args:
        connection: Open MySQL connection.

    Returns:
        pd.DataFrame: DataFrame containing the joined dataset.
    """

    query = """
    SELECT
      v.video_id,
      v.title,
      v.channel_id,
      c.channel_name,
      v.category,
      v.published_at,
      v.upload_hour,
      v.upload_day,
      v.view_count,
      v.like_count,
      v.comment_count,
      v.duration_seconds,
      v.tags,
      v.tag_count,
      v.title_length,
      v.title_word_count,
      v.engagement_rate,
      v.like_to_view_ratio,
      v.comment_to_view_ratio,
      c.subscriber_count,
      c.total_video_count,
      c.total_channel_views,
      c.country,
      v.collected_at
    FROM videos v
    JOIN channels c ON c.channel_id = v.channel_id
    """

    df = pd.read_sql(query, connection)
    return df


def standardize_category_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize category names to Title Case.
    """

    df = df.copy()
    df["category"] = df["category"].astype(str).str.title()
    return df


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add `performance_tier` and `duration_category` columns based on rules.
    """

    df = df.copy()

    df["performance_tier"] = np.select(
        [
            df["view_count"] >= 1_000_000,
            df["view_count"] >= 100_000,
            df["view_count"] >= 10_000,
            df["view_count"] < 10_000,
        ],
        ["Viral", "High", "Medium", "Low"],
        default="Low",
    )

    df["duration_category"] = np.select(
        [
            df["duration_seconds"] <= 180,
            df["duration_seconds"] <= 600,
            df["duration_seconds"] > 600,
        ],
        ["Short", "Medium", "Long"],
        default="Medium",
    )

    return df


def clean_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    """
    Apply all cleaning rules and return cleaned DataFrame + report metrics.
    """

    report = {}
    rows_before = len(df)
    report["rows_before"] = rows_before

    df = df.copy()

    # Remove videos with null/zero view_count.
    df = df[df["view_count"].notna()]
    df = df[df["view_count"] > 0]

    # Remove invalid/too-short durations (shorts/invalid).
    df = df[df["duration_seconds"].notna()]
    df = df[df["duration_seconds"] >= 30]

    report["rows_after_filters"] = len(df)

    # Fill missing tag fields.
    null_tags_before = int(df["tags"].isna().sum()) if "tags" in df.columns else 0
    null_tag_count_before = int(df["tag_count"].isna().sum()) if "tag_count" in df.columns else 0

    df["tags"] = df["tags"].fillna("no_tags")
    if "tag_count" in df.columns:
        df["tag_count"] = df["tag_count"].fillna(0).astype(int)

    report["nulls_fixed_tags"] = null_tags_before
    report["nulls_fixed_tag_count"] = null_tag_count_before

    # Standardize category names.
    df = standardize_category_names(df)

    # Cap engagement_rate outliers at 99th percentile.
    if df["engagement_rate"].notna().any():
        p99 = float(df["engagement_rate"].quantile(0.99))
        outlier_mask = df["engagement_rate"] > p99
        outliers_capped = int(outlier_mask.sum())
        df.loc[outlier_mask, "engagement_rate"] = p99
        report["engagement_p99"] = p99
        report["outliers_capped"] = outliers_capped
    else:
        report["engagement_p99"] = None
        report["outliers_capped"] = 0

    # Add derived tiers.
    df = add_derived_columns(df)

    report["rows_after"] = len(df)
    return df, report


def ensure_videos_cleaned_table(connection: MySQLConnection) -> None:
    """
    Create the `videos_cleaned` table if it doesn't exist.
    """

    ddl = """
    CREATE TABLE IF NOT EXISTS videos_cleaned (
      video_id VARCHAR(50) PRIMARY KEY,
      title VARCHAR(500),
      channel_id VARCHAR(50),
      channel_name VARCHAR(255),
      category VARCHAR(50),
      published_at DATETIME,
      upload_hour TINYINT,
      upload_day VARCHAR(20),
      view_count BIGINT,
      like_count BIGINT,
      comment_count BIGINT,
      duration_seconds INT,
      tags TEXT,
      tag_count SMALLINT,
      title_length SMALLINT,
      title_word_count SMALLINT,
      engagement_rate DECIMAL(10,4),
      like_to_view_ratio DECIMAL(10,4),
      comment_to_view_ratio DECIMAL(10,4),
      subscriber_count BIGINT,
      total_video_count INT,
      total_channel_views BIGINT,
      country VARCHAR(100),
      performance_tier VARCHAR(20),
      duration_category VARCHAR(20),
      collected_at DATETIME,
      INDEX idx_clean_category (category),
      INDEX idx_clean_upload_hour (upload_hour),
      INDEX idx_clean_engagement_rate (engagement_rate),
      INDEX idx_clean_published_at (published_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    cursor = connection.cursor()
    try:
        cursor.execute(ddl)
        connection.commit()
    finally:
        cursor.close()


def upsert_videos_cleaned(connection: MySQLConnection, df: pd.DataFrame) -> None:
    """
    Upsert cleaned rows into `videos_cleaned` using ON DUPLICATE KEY UPDATE.

    Args:
        connection: Open MySQL connection.
        df: Cleaned DataFrame.
    """

    if df.empty:
        print("[INFO] No cleaned rows to upsert into `videos_cleaned`.")
        return

    df_to_insert = df.where(pd.notnull(df), None).copy()

    sql = """
    INSERT INTO videos_cleaned (
      video_id, title, channel_id, channel_name, category, published_at, upload_hour, upload_day,
      view_count, like_count, comment_count, duration_seconds, tags, tag_count,
      title_length, title_word_count,
      engagement_rate, like_to_view_ratio, comment_to_view_ratio,
      subscriber_count, total_video_count, total_channel_views, country,
      performance_tier, duration_category, collected_at
    ) VALUES (
      %s, %s, %s, %s, %s, %s, %s, %s,
      %s, %s, %s, %s, %s, %s,
      %s, %s,
      %s, %s, %s,
      %s, %s, %s, %s,
      %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
      title = VALUES(title),
      channel_id = VALUES(channel_id),
      channel_name = VALUES(channel_name),
      category = VALUES(category),
      published_at = VALUES(published_at),
      upload_hour = VALUES(upload_hour),
      upload_day = VALUES(upload_day),
      view_count = VALUES(view_count),
      like_count = VALUES(like_count),
      comment_count = VALUES(comment_count),
      duration_seconds = VALUES(duration_seconds),
      tags = VALUES(tags),
      tag_count = VALUES(tag_count),
      title_length = VALUES(title_length),
      title_word_count = VALUES(title_word_count),
      engagement_rate = VALUES(engagement_rate),
      like_to_view_ratio = VALUES(like_to_view_ratio),
      comment_to_view_ratio = VALUES(comment_to_view_ratio),
      subscriber_count = VALUES(subscriber_count),
      total_video_count = VALUES(total_video_count),
      total_channel_views = VALUES(total_channel_views),
      country = VALUES(country),
      performance_tier = VALUES(performance_tier),
      duration_category = VALUES(duration_category),
      collected_at = VALUES(collected_at)
    """

    cursor = connection.cursor()
    try:
        rows = []
        for _, r in df_to_insert.iterrows():
            rows.append(
                (
                    r["video_id"],
                    r["title"],
                    r["channel_id"],
                    r["channel_name"],
                    r["category"],
                    r["published_at"].to_pydatetime() if hasattr(r["published_at"], "to_pydatetime") else r["published_at"],
                    int(r["upload_hour"]) if pd.notna(r["upload_hour"]) else None,
                    r["upload_day"],
                    int(r["view_count"]) if pd.notna(r["view_count"]) else None,
                    int(r["like_count"]) if pd.notna(r["like_count"]) else None,
                    int(r["comment_count"]) if pd.notna(r["comment_count"]) else None,
                    int(r["duration_seconds"]) if pd.notna(r["duration_seconds"]) else None,
                    r["tags"],
                    int(r["tag_count"]) if pd.notna(r["tag_count"]) else 0,
                    int(r["title_length"]) if pd.notna(r["title_length"]) else 0,
                    int(r["title_word_count"]) if pd.notna(r["title_word_count"]) else 0,
                    float(r["engagement_rate"]) if pd.notna(r["engagement_rate"]) else 0.0,
                    float(r["like_to_view_ratio"]) if pd.notna(r["like_to_view_ratio"]) else 0.0,
                    float(r["comment_to_view_ratio"]) if pd.notna(r["comment_to_view_ratio"]) else 0.0,
                    int(r["subscriber_count"]) if pd.notna(r["subscriber_count"]) else 0,
                    int(r["total_video_count"]) if pd.notna(r["total_video_count"]) else 0,
                    int(r["total_channel_views"]) if pd.notna(r["total_channel_views"]) else 0,
                    r["country"],
                    r["performance_tier"],
                    r["duration_category"],
                    r["collected_at"].to_pydatetime() if hasattr(r["collected_at"], "to_pydatetime") else r["collected_at"],
                )
            )

        cursor.executemany(sql, rows)
        connection.commit()
    finally:
        cursor.close()


def save_cleaned_csv(df: pd.DataFrame, out_path: str) -> None:
    """
    Save cleaned data to a CSV file for Power BI.
    """

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")


def run_cleaning() -> None:
    """
    Run the complete cleaning pipeline:
    - Load from MySQL
    - Clean + enrich
    - Upsert into `videos_cleaned`
    - Export to Power BI CSV
    """

    print("Loading raw data from MySQL...")
    connection = get_mysql_connection()
    try:
        raw_df = load_raw_data(connection)
        print(f"Loaded {len(raw_df)} raw rows.")

        print("Cleaning data...")
        cleaned_df, report = clean_data(raw_df)

        print("\n=== Cleaning Report ===")
        print(f"Rows before: {report.get('rows_before')}")
        print(f"Rows after filters: {report.get('rows_after_filters')}")
        print(f"Rows after: {report.get('rows_after')}")
        print(f"Nulls fixed - tags: {report.get('nulls_fixed_tags')}")
        print(f"Nulls fixed - tag_count: {report.get('nulls_fixed_tag_count')}")
        print(f"Outliers capped: {report.get('outliers_capped')} (p99={report.get('engagement_p99')})")

        print("Ensuring `videos_cleaned` table exists...")
        ensure_videos_cleaned_table(connection)

        print("Upserting cleaned rows into MySQL...")
        upsert_videos_cleaned(connection, cleaned_df)

        powerbi_csv_path = os.path.join("powerbi", "cleaned_data.csv")
        print(f"Saving cleaned CSV for Power BI to: {powerbi_csv_path}")
        save_cleaned_csv(cleaned_df, powerbi_csv_path)

        print("\nCleaning complete.")
    finally:
        connection.close()


if __name__ == "__main__":
    print("Starting data cleaning...")
    run_cleaning()

