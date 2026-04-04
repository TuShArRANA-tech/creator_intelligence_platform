<!-- Power BI import guide for Creator Intelligence Platform CSV exports. -->

# Power BI Guide

This guide explains how to import the CSV exports produced by `powerbi/export_for_powerbi.py` and how to model relationships and visuals.

## 1) Import CSV files

1. Open Power BI Desktop.
2. Go to **Home** -> **Get Data** -> **Text/CSV**.
3. Import these files from the `powerbi/` folder:
   - `videos_cleaned.csv`
   - `channels_summary.csv`
   - `category_summary.csv`
   - `timing_analysis.csv`
   - `performance_tiers.csv`

4. In the Power Query editor, verify:
   - `published_at` is typed as Date/Time (or Date).
   - `view_count`, `like_count`, `comment_count` are Whole Number.
   - `engagement_rate` is Decimal Number.
   - `upload_hour` is Whole Number.

5. Click **Close & Apply**.

## 2) Suggested relationships

Use these keys to connect summary tables to the fact table (`videos_cleaned`):

- `videos_cleaned[channel_id]` -> `channels_summary[channel_id]`
- `videos_cleaned[category]` -> `category_summary[category]`
- `videos_cleaned[category]` and `videos_cleaned[performance_tier]` -> `performance_tiers[category]` and `performance_tiers[performance_tier]`
- `videos_cleaned[upload_day]` and `videos_cleaned[upload_hour]` -> `timing_analysis[upload_day]` and `timing_analysis[upload_hour]`

Notes:
- For the composite relationships (two columns), create a composite key in Power BI if needed (recommended).
- Keep `videos_cleaned` as the main fact table for most visuals.

## 3) Suggested visuals (by dashboard page)

### Overview
- KPI cards: Total Videos, Avg Engagement Rate, Total Views, Most Active Category
- Bar chart: Video count by category
- Bar chart: Avg engagement rate by category
- Donut/pie chart: Performance tier distribution

### Timing Intelligence
- Heatmap: Avg engagement rate by `upload_day` vs `upload_hour` (use `timing_analysis`)
- Line chart: Avg views by `upload_hour`
- Text/card: “Best time to post” (max avg engagement combination)

### Creator Leaderboard
- Table: Top 20 channels by avg engagement (use `channels_summary`)
- Bar chart: Top 10 channels by total views
- Optional filter: Category slicer

### Video Explorer
- Slicer filters: category, performance tier, upload hour, min views
- Table or matrix: title, channel_name, category, view_count, engagement_rate, performance_tier

### Category Deep Dive
- Box plot or violin alternative: engagement distribution for selected category
- Table: Top 5 videos in category
- Text/card: best upload day + hour for selected category

## 4) DAX measures to create

### Avg Engagement Rate
```DAX
Avg Engagement Rate =
AVERAGE ( videos_cleaned[engagement_rate] )
```

### Viral Video %
```DAX
Viral Video % =
VAR ViralCount =
    CALCULATE ( COUNTROWS ( videos_cleaned ), videos_cleaned[performance_tier] = "Viral" )
VAR TotalCount =
    COUNTROWS ( videos_cleaned )
RETURN
DIVIDE ( ViralCount, TotalCount ) * 100
```

### Top Category by Views
```DAX
Top Category by Views =
VAR TopCatTable =
    TOPN (
        1,
        SUMMARIZE (
            category_summary,
            category_summary[category],
            "Views", category_summary[total_views]
        ),
        [Views],
        DESC
    )
RETURN
MAXX ( TopCatTable, [category_summary[category]] )
```

### MoM View Growth
```DAX
MoM View Growth % =
VAR CurrentMonthStart =
    DATE ( YEAR ( MAX ( videos_cleaned[published_at] ) ), MONTH ( MAX ( videos_cleaned[published_at] ) ), 1 )
VAR CurrentMonthEnd =
    EOMONTH ( CurrentMonthStart, 0 )
VAR PrevMonthStart =
    EDATE ( CurrentMonthStart, -1 )
VAR PrevMonthEnd =
    EOMONTH ( CurrentMonthStart, -1 )
VAR CurrentViews =
    CALCULATE (
        SUM ( videos_cleaned[view_count] ),
        videos_cleaned[published_at] >= CurrentMonthStart,
        videos_cleaned[published_at] <= CurrentMonthEnd
    )
VAR PrevViews =
    CALCULATE (
        SUM ( videos_cleaned[view_count] ),
        videos_cleaned[published_at] >= PrevMonthStart,
        videos_cleaned[published_at] <= PrevMonthEnd
    )
RETURN
DIVIDE ( CurrentViews - PrevViews, PrevViews ) * 100
```

## 5) Troubleshooting

- If you see blank charts, confirm that the data types were set correctly in Power Query.
- If relationships don’t filter as expected, create composite keys for two-column joins.

