-- Creator Intelligence Platform - SQL analysis queries
-- All queries are designed to run in MySQL Workbench (MySQL 8+ recommended)
-- They assume the cleaned fact table `videos_cleaned` exists.

-- 1. Top 10 videos by engagement_rate per category
--    Insight: identify which content performs best at driving likes+comments relative to views.
SELECT
  category,
  video_id,
  title,
  channel_name,
  engagement_rate
FROM (
  SELECT
    vc.*,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY engagement_rate DESC) AS rn
  FROM videos_cleaned vc
) ranked
WHERE rn <= 10
ORDER BY category, engagement_rate DESC
;

-- 2. Average engagement_rate, view_count, like_to_view_ratio by category
--    Insight: compare how categories differ not just in scale (views) but in engagement efficiency.
SELECT
  category,
  AVG(engagement_rate) AS avg_engagement_rate,
  AVG(view_count) AS avg_view_count,
  AVG(like_to_view_ratio) AS avg_like_to_view_ratio
FROM videos_cleaned
GROUP BY category
ORDER BY avg_engagement_rate DESC
;

-- 3. Best upload_hour for maximum average views
--    Insight: helps schedule publishing to maximize expected reach.
SELECT
  upload_hour,
  AVG(view_count) AS avg_view_count
FROM videos_cleaned
GROUP BY upload_hour
ORDER BY avg_view_count DESC
;

-- 4. Best upload_day for maximum average engagement_rate
--    Insight: find which days drive stronger viewer interaction rates.
SELECT
  upload_day,
  AVG(engagement_rate) AS avg_engagement_rate
FROM videos_cleaned
GROUP BY upload_day
ORDER BY avg_engagement_rate DESC
;

-- 5. Category distribution of performance_tier (Viral/High/Medium/Low)
--    Insight: shows whether a category tends to create breakthrough hits vs slower-performing content.
SELECT
  category,
  SUM(CASE WHEN performance_tier = 'Viral' THEN 1 ELSE 0 END) AS viral_videos,
  SUM(CASE WHEN performance_tier = 'High' THEN 1 ELSE 0 END) AS high_videos,
  SUM(CASE WHEN performance_tier = 'Medium' THEN 1 ELSE 0 END) AS medium_videos,
  SUM(CASE WHEN performance_tier = 'Low' THEN 1 ELSE 0 END) AS low_videos,
  COUNT(*) AS total_videos
FROM videos_cleaned
GROUP BY category
ORDER BY total_videos DESC
;

-- 6. Top 10 channels by average engagement_rate (minimum 5 videos in dataset)
--    Insight: finds creators that consistently generate strong engagement.
SELECT
  channel_id,
  channel_name,
  AVG(engagement_rate) AS avg_engagement_rate,
  COUNT(*) AS video_count
FROM videos_cleaned
GROUP BY channel_id, channel_name
HAVING COUNT(*) >= 5
ORDER BY avg_engagement_rate DESC
LIMIT 10
;

-- 7. Week-over-week view growth per category using LAG()
--    Insight: detect whether a category is trending upward or fading over time.
WITH weekly AS (
  SELECT
    category,
    -- Use ISO week (mode=3) and calculate a week_start date for readability.
    STR_TO_DATE(CONCAT(YEARWEEK(published_at, 3), ' Monday'), '%X%V %W') AS week_start,
    SUM(view_count) AS weekly_views
  FROM videos_cleaned
  GROUP BY category, YEARWEEK(published_at, 3)
)
SELECT
  category,
  week_start,
  weekly_views,
  LAG(weekly_views) OVER (PARTITION BY category ORDER BY week_start) AS previous_week_views,
  (weekly_views - LAG(weekly_views) OVER (PARTITION BY category ORDER BY week_start)) AS week_over_week_growth_views,
  ROUND(
    100.0 * (weekly_views - LAG(weekly_views) OVER (PARTITION BY category ORDER BY week_start))
    / NULLIF(LAG(weekly_views) OVER (PARTITION BY category ORDER BY week_start), 0),
    2
  ) AS week_over_week_growth_pct
FROM weekly
ORDER BY category, week_start
;

-- 8. Rank channels within each category by total views using RANK()
--    Insight: measure which creators dominate attention in each niche category.
SELECT
  category,
  channel_id,
  channel_name,
  SUM(view_count) AS total_views,
  RANK() OVER (PARTITION BY category ORDER BY SUM(view_count) DESC) AS views_rank
FROM videos_cleaned
GROUP BY category, channel_id, channel_name
ORDER BY category, views_rank
;

-- 9. Running total of videos collected per day using SUM() OVER (ORDER BY DATE(published_at))
--    Insight: understand dataset growth by posting history (useful for sampling freshness).
WITH daily AS (
  SELECT
    DATE(published_at) AS published_date,
    COUNT(*) AS videos_per_day
  FROM videos_cleaned
  GROUP BY DATE(published_at)
)
SELECT
  published_date,
  videos_per_day,
  SUM(videos_per_day) OVER (ORDER BY published_date) AS running_total_videos
FROM daily
ORDER BY published_date
;

-- 10. Videos with above-average engagement_rate within their own category (correlated subquery)
--     Insight: surfaces “standout” content relative to the category’s typical engagement.
SELECT
  vc.category,
  vc.video_id,
  vc.title,
  vc.channel_name,
  vc.engagement_rate
FROM videos_cleaned vc
WHERE vc.engagement_rate > (
  SELECT AVG(vc2.engagement_rate)
  FROM videos_cleaned vc2
  WHERE vc2.category = vc.category
)
ORDER BY vc.category, vc.engagement_rate DESC
;

-- 11. CTE: Calculate each channel's average views, then find channels performing above their category average
--     Insight: highlights channels that beat “typical” channel performance in their category.
WITH channel_avg AS (
  SELECT
    category,
    channel_id,
    channel_name,
    AVG(view_count) AS avg_channel_views
  FROM videos_cleaned
  GROUP BY category, channel_id, channel_name
),
category_avg AS (
  SELECT
    category,
    AVG(avg_channel_views) AS avg_category_views
  FROM channel_avg
  GROUP BY category
)
SELECT
  ca.category,
  ca.channel_id,
  ca.channel_name,
  ca.avg_channel_views,
  cat.avg_category_views
FROM channel_avg ca
JOIN category_avg cat ON cat.category = ca.category
WHERE ca.avg_channel_views > cat.avg_category_views
ORDER BY ca.category, ca.avg_channel_views DESC
;

-- 12. Top 5 most used tags across all videos (split comma-separated tags and count individually)
--     Insight: reveals recurring themes that correlate with engagement/view performance.
SELECT
  jt.tag AS tag,
  COUNT(*) AS tag_usage_count
FROM videos_cleaned vc
JOIN JSON_TABLE(
  CONCAT(
    '["',
    REPLACE(REPLACE(TRIM(vc.tags), '"', '\\"'), ',', '","'),
    '"]'
  ),
  '$[*]' COLUMNS (tag VARCHAR(255) PATH '$')
) jt
WHERE jt.tag IS NOT NULL AND TRIM(jt.tag) <> ''
GROUP BY jt.tag
ORDER BY tag_usage_count DESC
LIMIT 5
;

-- 13. Engagement rate distribution bucketed into 10 equal ranges using NTILE(10)
--     Insight: quantify how many videos fall into low vs high engagement performers.
WITH bucketed AS (
  SELECT
    NTILE(10) OVER (ORDER BY engagement_rate) AS engagement_bucket,
    engagement_rate
  FROM videos_cleaned
)
SELECT
  engagement_bucket,
  MIN(engagement_rate) AS bucket_min_engagement,
  MAX(engagement_rate) AS bucket_max_engagement,
  COUNT(*) AS video_count
FROM bucketed
GROUP BY engagement_bucket
ORDER BY engagement_bucket
;

-- 14. Correlation proxy: average views grouped by title_length buckets (0-30, 31-60, 61-100, 100+)
--     Insight: estimate whether longer/shorter titles tend to attract more views.
SELECT
  CASE
    WHEN title_length BETWEEN 0 AND 30 THEN '0-30'
    WHEN title_length BETWEEN 31 AND 60 THEN '31-60'
    WHEN title_length BETWEEN 61 AND 100 THEN '61-100'
    ELSE '100+'
  END AS title_length_bucket,
  AVG(view_count) AS avg_views,
  COUNT(*) AS video_count
FROM videos_cleaned
GROUP BY title_length_bucket
ORDER BY
  CASE title_length_bucket
    WHEN '0-30' THEN 1
    WHEN '31-60' THEN 2
    WHEN '61-100' THEN 3
    ELSE 4
  END
;

-- 15. Channels with the most consistent performance: lowest STDDEV of view_count (minimum 5 videos)
--     Insight: identify creators with stable output (less volatile view performance).
SELECT
  channel_id,
  channel_name,
  STDDEV_POP(view_count) AS stddev_view_count,
  COUNT(*) AS video_count
FROM videos_cleaned
GROUP BY channel_id, channel_name
HAVING COUNT(*) >= 5
ORDER BY stddev_view_count ASC
LIMIT 10
;

-- 16. Percentage share of total views per category using SUM() OVER() for grand total
--     Insight: measure category dominance by view volume.
WITH cat_views AS (
  SELECT category, SUM(view_count) AS category_views
  FROM videos_cleaned
  GROUP BY category
)
SELECT
  category,
  category_views,
  ROUND(100.0 * category_views / NULLIF(SUM(category_views) OVER (), 0), 2) AS pct_of_total_views
FROM cat_views
ORDER BY category_views DESC
;

-- 17. Videos posted in the last 30 days vs older: compare average engagement_rate
--     Insight: determine whether recent uploads perform differently than older ones.
SELECT
  CASE
    WHEN published_at >= NOW() - INTERVAL 30 DAY THEN 'Last 30 days'
    ELSE 'Older'
  END AS period_group,
  COUNT(*) AS video_count,
  AVG(engagement_rate) AS avg_engagement_rate,
  AVG(view_count) AS avg_views
FROM videos_cleaned
GROUP BY period_group
ORDER BY period_group
;

-- 18. Duration category performance: avg views and avg engagement by duration_category
--     Insight: quantify whether shorter or longer videos generate better engagement efficiency.
SELECT
  duration_category,
  COUNT(*) AS video_count,
  AVG(view_count) AS avg_views,
  AVG(engagement_rate) AS avg_engagement_rate
FROM videos_cleaned
GROUP BY duration_category
ORDER BY avg_views DESC
;

-- 19. Multi-level CTE: Step 1 get channel averages, Step 2 rank them, Step 3 filter top 20%
--     Insight: isolate the most effective channels (top quintile) within each category.
WITH channel_avg AS (
  SELECT
    category,
    channel_id,
    channel_name,
    AVG(view_count) AS avg_views,
    AVG(engagement_rate) AS avg_engagement_rate,
    COUNT(*) AS video_count,
    SUM(view_count) AS total_views
  FROM videos_cleaned
  GROUP BY category, channel_id, channel_name
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY avg_engagement_rate DESC) AS rn,
    COUNT(*) OVER (PARTITION BY category) AS channels_in_category
  FROM channel_avg
)
SELECT
  category,
  channel_id,
  channel_name,
  avg_views,
  avg_engagement_rate,
  video_count,
  total_views,
  rn
FROM ranked
WHERE rn <= CEIL(channels_in_category * 0.2)
ORDER BY category, rn
;

-- 20. Full summary report per category in one query (subqueries):
--     total videos, avg views, avg engagement, top channel name, best upload hour
--     Insight: provides a compact “category scorecard” for decision-making.
SELECT
  c.category,
  (SELECT COUNT(*)
   FROM videos_cleaned vc2
   WHERE vc2.category = c.category) AS total_videos,
  (SELECT AVG(vc2.view_count)
   FROM videos_cleaned vc2
   WHERE vc2.category = c.category) AS avg_views,
  (SELECT AVG(vc2.engagement_rate)
   FROM videos_cleaned vc2
   WHERE vc2.category = c.category) AS avg_engagement_rate,
  (SELECT vc3.channel_name
   FROM videos_cleaned vc3
   WHERE vc3.category = c.category
   GROUP BY vc3.channel_id, vc3.channel_name
   ORDER BY SUM(vc3.view_count) DESC
   LIMIT 1) AS top_channel_name,
  (SELECT vc4.upload_hour
   FROM videos_cleaned vc4
   WHERE vc4.category = c.category
   GROUP BY vc4.upload_hour
   ORDER BY AVG(vc4.view_count) DESC
   LIMIT 1) AS best_upload_hour
FROM (SELECT DISTINCT category FROM videos_cleaned) c
ORDER BY c.category
;
