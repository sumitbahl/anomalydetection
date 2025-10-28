-- Step 1: Truncate timestamps to hours
WITH truncated AS (
  SELECT
    TIMESTAMP_TRUNC(Datetime_UTC, HOUR) AS Datetime
  FROM `anomaly_detection.unaggregated_volume`
  WHERE Datetime_UTC IS NOT NULL
),

-- Step 2: Aggregate hourly counts
hourly_aggregated AS (
  SELECT
    Datetime,
    COUNT(*) AS Volume,
    EXTRACT(HOUR FROM Datetime) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM Datetime) AS day_of_week
  FROM truncated
  GROUP BY Datetime
)

-- Step 3: Detect anomalies
SELECT *
FROM ML.DETECT_ANOMALIES(
    MODEL `anomaly_detection.app_trading_volume_model_from_unaggregated`,
    STRUCT(0.9999 AS anomaly_prob_threshold),
    TABLE hourly_aggregated
)
--WHERE is_anomaly = TRUE
ORDER BY anomaly_probability DESC;



------------- Below simple query worked ------------
SELECT *
FROM ML.DETECT_ANOMALIES(
  MODEL `anomaly_detection.app_trading_volume_model_from_unaggregated`,
  STRUCT(0.999 AS anomaly_prob_threshold)
)
WHERE is_anomaly = TRUE
  AND ABS(Volume - (lower_bound + upper_bound)/2) > 5
