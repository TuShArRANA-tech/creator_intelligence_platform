<!-- Creator Intelligence Platform README (Windows setup + pipeline overview). -->

# Creator Intelligence Platform

Creator Intelligence Platform is a social media analytics system that:
- Collects live YouTube video and channel data using the YouTube Data API v3
- Stores raw data in MySQL
- Cleans and enriches data with SQL + Python
- Explores results via an EDA Jupyter notebook
- Visualizes insights in a Streamlit web dashboard
- Exports Power BI-ready CSVs for deeper reporting

## Architecture Diagram (ASCII)

```
YouTube Data API v3
        |
        | 1) Collect (Python)
        v
data_collection/youtube_collector.py
        |
        v
MySQL (channels, videos)
        |
        | 2) Clean + Enrich (Python)
        v
data_cleaning/cleaner.py  -->  videos_cleaned (MySQL)
        |                         |
        |                         v
        |                    powerbi/cleaned_data.csv
        |
        | 3) Analyze (SQL + Notebook)
        v
sql_analysis/queries.sql and eda/analysis.ipynb
        |
        | 4) Visualize (Streamlit)
        v
dashboard/app.py
        |
        | 5) Export (CSV for Power BI)
        v
powerbi/export_for_powerbi.py  -->  powerbi/*.csv
        |
        v
Power BI reports
```

## Windows Setup (Step by Step)

### 1) Get a YouTube API key (exact steps)
1. Go to the **Google Cloud Console**: https://console.cloud.google.com/
2. Create (or select) a project.
3. In the left navigation, open **APIs & Services** -> **Library**.
4. Search for **YouTube Data API v3**.
5. Click **Enable**.
6. In the left navigation, open **APIs & Services** -> **Credentials**.
7. Click **+ Create Credentials** -> **API key**.
8. Copy the API key value.
9. (Recommended) In the API key settings, set restrictions:
   - Restrict to the **YouTube Data API v3** (and optionally specific IPs/domains).

### 2) Create the MySQL database
1. Open MySQL Workbench.
2. Create a database named `creator_intelligence`.
3. Run the schema script: `database/schema.sql`.

### 3) Install Python dependencies
From the project root:
1. Open PowerShell.
2. Run:
   - `pip install -r requirements.txt`

### 4) Fill in the `.env` file
1. Copy `.env.example` to `.env`.
2. Edit it and set:
   - `YOUTUBE_API_KEY` to your key
   - `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`

### 5) Run data collection
Run the collector to pull videos and channels into MySQL:
1. PowerShell from the project root:
2. Run:
   - `python data_collection/youtube_collector.py`

### 6) Run cleaning
1. Run:
   - `python data_cleaning/cleaner.py`

### 7) Run Streamlit dashboard
1. Run:
   - `streamlit run dashboard/app.py`

2. Open the provided local URL in your browser.

### 8) Open the Jupyter notebook
1. Run Jupyter:
   - `jupyter notebook`
2. Open:
   - `eda/analysis.ipynb`

### 9) Import into Power BI
1. Run:
   - `python powerbi/export_for_powerbi.py`
2. In Power BI Desktop, use **Get Data** -> **Text/CSV** and import the CSVs from the `powerbi/` folder.
3. Follow the detailed instructions in `powerbi/POWERBI_GUIDE.md`.

## Folder Structure

The project is organized by pipeline layer:

- `data_collection/`: YouTube API collection logic and raw backup
- `database/`: MySQL schema and reusable connection/upsert helpers
- `data_cleaning/`: Cleaning pipeline that creates `videos_cleaned` and exports CSV
- `sql_analysis/`: MySQL Workbench-ready SQL queries (window functions + CTEs)
- `eda/`: Jupyter notebook with plots saved to `eda/charts/`
- `dashboard/`: Streamlit app with Plotly charts
- `powerbi/`: Power BI CSV exports and import guide

## Sample Screenshots

Placeholder section:
- Overview page: (screenshot_placeholder_overview.png)
- Timing Intelligence page: (screenshot_placeholder_timing.png)
- Creator Leaderboard page: (screenshot_placeholder_leaderboard.png)
- Video Explorer page: (screenshot_placeholder_explorer.png)
- Category Deep Dive page: (screenshot_placeholder_deep_dive.png)

## Skills Demonstrated

Python, Pandas, MySQL, SQL Window Functions, CTEs, EDA, Streamlit, Power BI, Data Pipeline, API Integration

