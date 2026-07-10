-- StreamPulse Gold-layer analysis examples
-- Dialect: BigQuery Standard SQL
-- Replace `your_project.streaming_analytics` with the deployed dataset.

-- 1) Rank content by watch hours while preserving quality context.
SELECT
  event_date,
  content_id,
  ANY_VALUE(content_title) AS content_title,
  SUM(watch_hours) AS watch_hours,
  SUM(plays) AS playback_starts,
  SAFE_DIVIDE(SUM(completes), SUM(plays)) AS completion_rate,
  SAFE_DIVIDE(SUM(qoe_score * event_count), SUM(event_count)) AS weighted_qoe_score
FROM `your_project.streaming_analytics.gold_content_daily`
WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY event_date, content_id
QUALIFY DENSE_RANK() OVER (
  PARTITION BY event_date
  ORDER BY SUM(watch_hours) DESC
) <= 10
ORDER BY event_date DESC, watch_hours DESC;


-- 2) Detect device/market combinations with a material QoE regression.
WITH hourly AS (
  SELECT
    window_start,
    country,
    device_type,
    SAFE_DIVIDE(SUM(qoe_score * event_count), SUM(event_count)) AS qoe_score,
    SAFE_DIVIDE(SUM(playback_errors), SUM(plays)) AS error_rate
  FROM `your_project.streaming_analytics.gold_platform_hourly`
  WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
  GROUP BY window_start, country, device_type
),
with_baseline AS (
  SELECT
    *,
    AVG(qoe_score) OVER (
      PARTITION BY country, device_type
      ORDER BY window_start
      ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
    ) AS prior_24h_qoe
  FROM hourly
)
SELECT
  *,
  qoe_score - prior_24h_qoe AS qoe_delta
FROM with_baseline
WHERE prior_24h_qoe IS NOT NULL
  AND qoe_score < prior_24h_qoe - 10
ORDER BY qoe_delta;


-- 3) Compare engagement and reliability by subscription tier.
SELECT
  subscription_tier,
  SUM(plays) AS playback_starts,
  SUM(unique_viewers) AS viewer_day_segments,
  SUM(watch_hours) AS watch_hours,
  SAFE_DIVIDE(SUM(completes), SUM(plays)) AS completion_rate,
  SAFE_DIVIDE(SUM(playback_errors), SUM(plays)) AS error_rate,
  SAFE_DIVIDE(SUM(qoe_score * event_count), SUM(event_count)) AS weighted_qoe_score
FROM `your_project.streaming_analytics.gold_content_daily`
WHERE event_date BETWEEN @start_date AND @end_date
GROUP BY subscription_tier
ORDER BY watch_hours DESC;


-- 4) Session-level playback funnel from the Silver event table.
WITH sessions AS (
  SELECT
    session_id,
    ANY_VALUE(content_id) AS content_id,
    ANY_VALUE(device_type) AS device_type,
    MIN(event_ts) AS session_started_at,
    COUNTIF(event_type = 'playback_started') > 0 AS started,
    COUNTIF(event_type = 'playback_progress') > 0 AS progressed,
    COUNTIF(event_type = 'playback_completed') > 0 AS completed,
    COUNTIF(event_type = 'playback_error') > 0 AS failed,
    SUM(COALESCE(watch_seconds, 0)) AS watch_seconds
  FROM `your_project.streaming_analytics.silver_playback_events`
  WHERE DATE(event_ts) = @event_date
  GROUP BY session_id
)
SELECT
  device_type,
  COUNT(*) AS sessions,
  COUNTIF(started) AS starts,
  COUNTIF(progressed) AS progressed_sessions,
  COUNTIF(completed) AS completions,
  COUNTIF(failed) AS failed_sessions,
  APPROX_QUANTILES(watch_seconds, 100)[OFFSET(50)] AS median_watch_seconds
FROM sessions
GROUP BY device_type
ORDER BY sessions DESC;

