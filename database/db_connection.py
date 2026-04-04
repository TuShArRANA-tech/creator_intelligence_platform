"""
Creator Intelligence Platform - MySQL connection + upsert helpers.

This module loads database credentials from `.env` (via python-dotenv) and provides:
- A reusable MySQL connection function
- ON DUPLICATE KEY UPDATE upsert functions for `channels` and `videos`
"""

from __future__ import annotations

import os
from typing import Iterable, Mapping, Sequence, Any, Optional

import mysql.connector
from dotenv import load_dotenv


def get_mysql_connection() -> mysql.connector.MySQLConnection:
    """
    Create and return a reusable MySQL connection using credentials from `.env`.

    Returns:
        mysql.connector.MySQLConnection: An open connection to the MySQL database.
    """

    # Load env vars from `.env` into the process environment.
    load_dotenv()

    host = os.getenv("DB_HOST")
    port = int(os.getenv("DB_PORT", "3306"))
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")

    if not all([host, user, password, db_name]):
        raise RuntimeError(
            "Missing required database env vars. Ensure DB_HOST, DB_PORT, DB_USER, "
            "DB_PASSWORD, and DB_NAME are set in .env."
        )

    conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db_name,
    )
    return conn


def _executemany_upsert(
    cursor: mysql.connector.cursor.MySQLCursor,
    sql: str,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    """
    Helper to call cursor.executemany for a list of row dicts.

    Args:
        cursor: Active MySQL cursor.
        sql: Parameterized SQL with placeholders (%s).
        rows: Sequence of dicts matching the parameter order for the query.
    """

    if not rows:
        return

    # Preserve parameter ordering by using the keys in the first dict.
    # All rows are expected to have the same set of keys.
    first_keys = list(rows[0].keys())
    values = [[row.get(k) for k in first_keys] for row in rows]
    cursor.executemany(sql, values)


def upsert_channels(
    connection: mysql.connector.MySQLConnection,
    channels: Iterable[Mapping[str, Any]],
) -> None:
    """
    Upsert channel records into the `channels` table using ON DUPLICATE KEY UPDATE.

    Args:
        connection: Open MySQL connection.
        channels: Iterable of channel dicts with required keys.
    """

    channel_rows = list(channels)
    if not channel_rows:
        return

    sql = """
    INSERT INTO channels (
      channel_id, channel_name, subscriber_count, total_video_count,
      total_channel_views, country, collected_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      channel_name = VALUES(channel_name),
      subscriber_count = VALUES(subscriber_count),
      total_video_count = VALUES(total_video_count),
      total_channel_views = VALUES(total_channel_views),
      country = VALUES(country),
      collected_at = VALUES(collected_at)
    """

    cursor = connection.cursor()
    try:
        # Force consistent ordering for values.
        ordered_rows = [
            {
                "channel_id": r.get("channel_id"),
                "channel_name": r.get("channel_name"),
                "subscriber_count": r.get("subscriber_count"),
                "total_video_count": r.get("total_video_count"),
                "total_channel_views": r.get("total_channel_views"),
                "country": r.get("country"),
                "collected_at": r.get("collected_at"),
            }
            for r in channel_rows
        ]

        _executemany_upsert(
            cursor=cursor,
            sql=sql,
            rows=ordered_rows,
        )
        connection.commit()
    finally:
        cursor.close()


def upsert_videos(
    connection: mysql.connector.MySQLConnection,
    videos: Iterable[Mapping[str, Any]],
) -> None:
    """
    Upsert video records into the `videos` table using ON DUPLICATE KEY UPDATE.

    Args:
        connection: Open MySQL connection.
        videos: Iterable of video dicts with required keys.
    """

    video_rows = list(videos)
    if not video_rows:
        return

    sql = """
    INSERT INTO videos (
      video_id, title, channel_id, category, published_at, upload_hour, upload_day,
      view_count, like_count, comment_count, duration_seconds, tags, tag_count,
      title_length, title_word_count,
      engagement_rate, like_to_view_ratio, comment_to_view_ratio,
      collected_at
    ) VALUES (
      %s, %s, %s, %s, %s, %s, %s,
      %s, %s, %s, %s, %s, %s,
      %s, %s,
      %s, %s, %s,
      %s
    )
    ON DUPLICATE KEY UPDATE
      title = VALUES(title),
      channel_id = VALUES(channel_id),
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
      collected_at = VALUES(collected_at)
    """

    cursor = connection.cursor()
    try:
        ordered_rows = [
            {
                "video_id": r.get("video_id"),
                "title": r.get("title"),
                "channel_id": r.get("channel_id"),
                "category": r.get("category"),
                "published_at": r.get("published_at"),
                "upload_hour": r.get("upload_hour"),
                "upload_day": r.get("upload_day"),
                "view_count": r.get("view_count"),
                "like_count": r.get("like_count"),
                "comment_count": r.get("comment_count"),
                "duration_seconds": r.get("duration_seconds"),
                "tags": r.get("tags"),
                "tag_count": r.get("tag_count"),
                "title_length": r.get("title_length"),
                "title_word_count": r.get("title_word_count"),
                "engagement_rate": r.get("engagement_rate"),
                "like_to_view_ratio": r.get("like_to_view_ratio"),
                "comment_to_view_ratio": r.get("comment_to_view_ratio"),
                "collected_at": r.get("collected_at"),
            }
            for r in video_rows
        ]

        _executemany_upsert(
            cursor=cursor,
            sql=sql,
            rows=ordered_rows,
        )
        connection.commit()
    finally:
        cursor.close()


def upsert_channels_and_videos(
    connection: mysql.connector.MySQLConnection,
    channels: Iterable[Mapping[str, Any]],
    videos: Iterable[Mapping[str, Any]],
) -> None:
    """
    Upsert both channels and videos in a safe order.

    Args:
        connection: Open MySQL connection.
        channels: Iterable of channel dicts.
        videos: Iterable of video dicts.
    """

    upsert_channels(connection=connection, channels=channels)
    upsert_videos(connection=connection, videos=videos)

