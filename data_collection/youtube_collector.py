"""
Creator Intelligence Platform - YouTube Data API v3 collector.

Collects live YouTube video + channel data across 6 categories:
Tech, Finance, Gaming, Fitness, Education, Comedy.

Writes into MySQL using ON DUPLICATE KEY UPDATE (re-running avoids duplicates)
and saves raw API responses to `data_collection/raw_backup.json` as a backup.
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from math import ceil
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from database.db_connection import get_mysql_connection, upsert_channels_and_videos


API_DELAY_SECONDS = 1.0
VIDEO_BATCH_SIZE = 50
CHANNEL_BATCH_SIZE = 50


@dataclass(frozen=True)
class CategorySpec:
    name: str
    query: str


def parse_iso8601_duration_to_seconds(duration_iso: str) -> int:
    """
    Convert an ISO 8601 duration (e.g. 'PT1H2M3S') into seconds.

    Args:
        duration_iso: ISO 8601 duration string from YouTube (contentDetails.duration).

    Returns:
        int: Total duration in seconds.
    """

    if not duration_iso:
        return 0

    # Supports: PT#H#M#S with any component optional.
    match = re.match(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$", duration_iso)
    if not match:
        return 0

    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


def compute_upload_day(published_at: datetime) -> str:
    """
    Extract the day name (e.g., 'Monday') from a published datetime.
    Uses the datetime's current timezone; caller should pass the desired timezone.
    """

    return published_at.strftime("%A")


def safe_int(value: Any, default: int = 0) -> int:
    """
    Convert potentially None/str values into an int safely.
    """

    try:
        if value is None:
            return default
        return int(value)
    except (ValueError, TypeError):
        return default


def append_raw_backup(backup_path: str, record: Dict[str, Any]) -> None:
    """
    Append a raw API response record as a JSON line.

    Notes:
        This writes NDJSON (one JSON object per line) to keep the backup file append-friendly.
    """

    os.makedirs(os.path.dirname(backup_path), exist_ok=True)
    with open(backup_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def youtube_api_call_safely(func, backup_record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Execute a YouTube API call safely and return the JSON response on success.

    Args:
        func: Callable that executes the YouTube API request and returns a dict.
        backup_record: Metadata to store alongside the raw response.

    Returns:
        Optional[Dict[str, Any]]: Response dict if successful, else None.
    """

    backup_path = os.path.join("data_collection", "raw_backup.json")
    try:
        response = func()
        append_raw_backup(backup_path, {**backup_record, "response": response})
        return response
    except HttpError as e:
        print(f"[API ERROR] {backup_record.get('endpoint')} - HTTP error: {e}")
    except Exception as e:
        print(f"[API ERROR] {backup_record.get('endpoint')} - {type(e).__name__}: {e}")
    finally:
        time.sleep(API_DELAY_SECONDS)
    return None


def extract_video_row(video_item: Dict[str, Any], category: str, collected_at: datetime) -> Optional[Dict[str, Any]]:
    """
    Transform a YouTube videos.list item into the `videos` table row shape.

    Args:
        video_item: Item from videos.list response.
        category: One of the allowed 6 categories.
        collected_at: Datetime when we pulled this data.

    Returns:
        Optional[Dict[str, Any]]: Row dict ready for MySQL insert, or None if missing critical fields.
    """

    snippet = video_item.get("snippet") or {}
    stats = video_item.get("statistics") or {}
    content = video_item.get("contentDetails") or {}

    video_id = video_item.get("id")
    title = snippet.get("title")
    channel_id = snippet.get("channelId")
    channel_name = (snippet.get("channelTitle") or "").strip()
    published_at_iso = snippet.get("publishedAt")

    if not video_id or not title or not channel_id or not published_at_iso:
        return None

    try:
        published_at = datetime.fromisoformat(published_at_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None
    upload_hour = int(published_at.hour)
    upload_day = compute_upload_day(published_at)

    view_count = safe_int(stats.get("viewCount"))
    like_count = safe_int(stats.get("likeCount"))
    comment_count = safe_int(stats.get("commentCount"))

    duration_seconds = parse_iso8601_duration_to_seconds(content.get("duration"))
    tags_list = snippet.get("tags")
    if tags_list:
        tags_str = ",".join([str(t).strip() for t in tags_list if str(t).strip()])
        tag_count = len([t for t in tags_list if str(t).strip()])
    else:
        tags_str = None
        tag_count = 0

    title_length = len(title)
    title_word_count = len([w for w in title.split() if w.strip()])

    if view_count > 0:
        engagement_rate = round(((like_count + comment_count) / view_count) * 100.0, 4)
        like_to_view_ratio = round((like_count / view_count) * 100.0, 4)
        comment_to_view_ratio = round((comment_count / view_count) * 100.0, 4)
    else:
        engagement_rate = 0.0
        like_to_view_ratio = 0.0
        comment_to_view_ratio = 0.0

    return {
        "video_id": str(video_id),
        "title": str(title),
        "channel_id": str(channel_id),
        # Collected for completeness; persisted at channel-level in `channels`.
        "channel_name": str(channel_name),
        "category": str(category),
        "published_at": published_at.replace(tzinfo=None),
        "upload_hour": upload_hour,
        "upload_day": upload_day,
        "view_count": view_count,
        "like_count": like_count,
        "comment_count": comment_count,
        "duration_seconds": duration_seconds,
        "tags": tags_str,
        "tag_count": tag_count,
        "title_length": title_length,
        "title_word_count": title_word_count,
        "engagement_rate": float(engagement_rate),
        "like_to_view_ratio": float(like_to_view_ratio),
        "comment_to_view_ratio": float(comment_to_view_ratio),
        "collected_at": collected_at.replace(tzinfo=None),
    }


def extract_channel_row(channel_item: Dict[str, Any], collected_at: datetime) -> Optional[Dict[str, Any]]:
    """
    Transform a YouTube channels.list item into the `channels` table row shape.
    """

    cid = channel_item.get("id")
    snippet = channel_item.get("snippet") or {}
    stats = channel_item.get("statistics") or {}
    if not cid:
        return None

    country = snippet.get("country")
    return {
        "channel_id": str(cid),
        "channel_name": (snippet.get("title") or "").strip(),
        "subscriber_count": safe_int(stats.get("subscriberCount"), default=0),
        "total_video_count": safe_int(stats.get("videoCount"), default=0),
        "total_channel_views": safe_int(stats.get("viewCount"), default=0),
        "country": country,
        "collected_at": collected_at.replace(tzinfo=None),
    }


def chunked(seq: List[str], size: int) -> Iterable[List[str]]:
    """
    Yield successive chunks from a list.
    """

    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def collect_for_category(
    youtube,
    category: CategorySpec,
    connection,
    target_count: int,
    seen_video_ids: Set[str],
    fetched_channel_ids: Set[str],
) -> Tuple[int, int]:
    """
    Collect videos for a single category until `target_count` is reached (or results end).

    Returns:
        (collected_videos, collected_channels)
    """

    print(f"\n=== Collecting category: {category.name} ===")
    collected_videos = 0
    collected_channels = 0

    collected_at = datetime.now(timezone.utc)

    page_token: Optional[str] = None
    search_attempts = 0

    while collected_videos < target_count:
        # Fetch a page of candidate videos.
        search_body = {
            "endpoint": "search.list",
            "category": category.name,
            "query": category.query,
            "page_token": page_token,
        }

        search_request = youtube.search().list(
            q=category.query,
            type="video",
            part="id",
            maxResults=50,
            pageToken=page_token,
        )
        search_response = youtube_api_call_safely(
            func=search_request.execute,
            backup_record=search_body,
        )

        if not search_response:
            search_attempts += 1
            if search_attempts > 5:
                print(f"[WARN] Too many failed search attempts for {category.name}. Stopping this category.")
                break
            continue

        video_ids: List[str] = []
        for item in search_response.get("items", []):
            vid = (item.get("id") or {}).get("videoId")
            if vid and vid not in seen_video_ids:
                seen_video_ids.add(vid)
                video_ids.append(vid)

        if not video_ids:
            print(f"[INFO] No new video IDs found for {category.name} (or already seen).")
            # If page token exists, keep moving; otherwise break.
        else:
            # Fetch details/statistics for these videos (batch).
            for id_chunk in chunked(video_ids, VIDEO_BATCH_SIZE):
                videos_body = {
                    "endpoint": "videos.list",
                    "category": category.name,
                    "video_ids_sample": id_chunk[:3],
                }

                try:
                    videos_request = youtube.videos().list(
                        id=",".join(id_chunk),
                        part="snippet,statistics,contentDetails",
                        maxResults=len(id_chunk),
                    )
                except Exception as e:
                    print(f"[API ERROR] Failed building videos request: {e}")
                    continue

                videos_response = youtube_api_call_safely(
                    func=videos_request.execute,
                    backup_record=videos_body,
                )

                if not videos_response:
                    print(f"[WARN] videos.list failed for a batch in {category.name}; skipping this batch.")
                    continue

                video_items = videos_response.get("items", []) or []
                channel_ids_to_fetch: Set[str] = set()
                video_rows: List[Dict[str, Any]] = []

                for item in video_items:
                    row = extract_video_row(item, category=category.name, collected_at=collected_at)
                    if not row:
                        continue
                    video_rows.append(row)
                    channel_ids_to_fetch.add(row["channel_id"])

                # Upsert new channels first.
                new_channel_ids = [cid for cid in channel_ids_to_fetch if cid not in fetched_channel_ids]
                if new_channel_ids:
                    for c_chunk in chunked(new_channel_ids, CHANNEL_BATCH_SIZE):
                        channels_body = {
                            "endpoint": "channels.list",
                            "category": category.name,
                            "channel_ids_sample": c_chunk[:3],
                        }
                        channels_request = youtube.channels().list(
                            id=",".join(c_chunk),
                            part="snippet,statistics",
                            maxResults=len(c_chunk),
                        )
                        channels_response = youtube_api_call_safely(
                            func=channels_request.execute,
                            backup_record=channels_body,
                        )
                        if not channels_response:
                            print(f"[WARN] channels.list failed for a chunk in {category.name}; skipping those channels.")
                            continue

                        channel_rows: List[Dict[str, Any]] = []
                        for c_item in channels_response.get("items", []) or []:
                            c_row = extract_channel_row(c_item, collected_at=collected_at)
                            if c_row:
                                channel_rows.append(c_row)

                        if channel_rows:
                            try:
                                upsert_channels_and_videos(connection=connection, channels=channel_rows, videos=[])
                                for c_row in channel_rows:
                                    fetched_channel_ids.add(c_row["channel_id"])
                                collected_channels += len(channel_rows)
                            except Exception as e:
                                print(f"[DB ERROR] Failed upserting channels for {category.name}: {type(e).__name__}: {e}")

                if video_rows:
                    # Enforce FK constraint: only insert videos whose channel rows we successfully fetched.
                    video_rows_to_insert = [r for r in video_rows if r.get("channel_id") in fetched_channel_ids]
                    skipped = len(video_rows) - len(video_rows_to_insert)
                    if skipped > 0:
                        print(f"[INFO] Skipping {skipped} videos due to missing channel data (FK enforcement).")

                    if video_rows_to_insert:
                        try:
                            # Re-running uses ON DUPLICATE KEY UPDATE, so we can safely upsert.
                            upsert_channels_and_videos(connection=connection, channels=[], videos=video_rows_to_insert)
                            collected_videos += len(video_rows_to_insert)
                        except Exception as e:
                            print(f"[DB ERROR] Failed upserting videos for {category.name}: {type(e).__name__}: {e}")

                overall = f"{category.name}: {collected_videos}/{target_count}"
                print(f"[PROGRESS] {overall} (total channels upserted so far: {collected_channels})")

                # Stop early if we crossed target.
                if collected_videos >= target_count:
                    break

        page_token = search_response.get("nextPageToken")
        if not page_token:
            print(f"[INFO] No more pages for {category.name}. Collected {collected_videos} videos.")
            break

    return collected_videos, collected_channels


def collect_youtube_data(total_target_videos: int = 500) -> None:
    """
    Main entry point to collect YouTube data for the platform.

    Args:
        total_target_videos: Total videos across all categories to aim for.
    """

    load_dotenv()
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        raise RuntimeError("Missing YOUTUBE_API_KEY in .env")

    categories: List[CategorySpec] = [
        CategorySpec(name="Tech", query="technology news"),
        CategorySpec(name="Finance", query="finance investing"),
        CategorySpec(name="Gaming", query="gaming walkthrough"),
        CategorySpec(name="Fitness", query="workout routine"),
        CategorySpec(name="Education", query="education tutorial"),
        CategorySpec(name="Comedy", query="stand up comedy"),
    ]

    per_category_target = int(ceil(total_target_videos / len(categories)))

    connection = get_mysql_connection()
    youtube = build("youtube", "v3", developerKey=api_key)

    seen_video_ids: Set[str] = set()
    fetched_channel_ids: Set[str] = set()

    try:
        total_collected = 0
        total_channels = 0

        for c in categories:
            collected, channels_collected = collect_for_category(
                youtube=youtube,
                category=c,
                connection=connection,
                target_count=per_category_target,
                seen_video_ids=seen_video_ids,
                fetched_channel_ids=fetched_channel_ids,
            )
            total_collected += collected
            total_channels += channels_collected
            print(f"[DONE] {c.name}: collected {collected} videos, upserted {channels_collected} channels.")

        print(
            f"\n=== Collection complete ===\n"
            f"Total videos collected (best effort): {total_collected}\n"
            f"Total channels upserted (best effort): {total_channels}\n"
        )
    finally:
        connection.close()


if __name__ == "__main__":
    print("Starting YouTube data collection...")
    collect_youtube_data(total_target_videos=500)
