-- Creator Intelligence Platform - MySQL schema (channels + videos)
-- Run in MySQL Workbench (MySQL 8+ recommended for window function features in analysis queries).

CREATE TABLE IF NOT EXISTS channels (
  channel_id VARCHAR(50) PRIMARY KEY,
  channel_name VARCHAR(255),
  subscriber_count BIGINT,
  total_video_count INT,
  total_channel_views BIGINT,
  country VARCHAR(100),
  collected_at DATETIME,
  INDEX idx_subscriber_count (subscriber_count)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS videos (
  video_id VARCHAR(50) PRIMARY KEY,
  title VARCHAR(500),
  channel_id VARCHAR(50) NOT NULL,
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
  collected_at DATETIME,
  INDEX idx_category (category),
  INDEX idx_upload_hour (upload_hour),
  INDEX idx_engagement_rate (engagement_rate),
  INDEX idx_published_at (published_at),
  CONSTRAINT fk_videos_channel
    FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
    ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
