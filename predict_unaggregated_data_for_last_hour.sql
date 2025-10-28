WITH last_hour AS (
  SELECT
    TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), HOUR) AS Datetime,
    COUNT(*) AS Volume,
    EXTRACT(HOUR FROM TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), HOUR)) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), HOUR)) AS day_of_week
  FROM `anomaly_detection.unaggregated_volume`
  WHERE Datetime_UTC >= TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR), INTERVAL 1 HOUR)
    AND Datetime_UTC < TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR)
)

SELECT
  Datetime,
  Volume,
  anomaly_probability,
  lower_bound,
  upper_bound,
  CASE
    WHEN ABS(Volume - (lower_bound + upper_bound)/2) > 50 THEN TRUE
    ELSE FALSE
  END AS is_custom_anomaly
FROM ML.DETECT_ANOMALIES(
  MODEL `anomaly_detection.app_trading_volume_model_from_unaggregated`,
  STRUCT(0.95 AS anomaly_prob_threshold),
  TABLE last_hour
);
