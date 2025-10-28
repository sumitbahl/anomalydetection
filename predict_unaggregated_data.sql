WITH new_data AS (
  SELECT
    TIMESTAMP("2025-10-01 00:00:00 UTC") AS Datetime,
    600 AS Volume,
    EXTRACT(HOUR FROM TIMESTAMP("2025-10-01 00:00:00 UTC")) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM TIMESTAMP("2025-10-01 00:00:00 UTC")) AS day_of_week
)

SELECT
  Datetime,
  Volume,
  anomaly_probability,
  is_anomaly,
  lower_bound,
  upper_bound
FROM ML.DETECT_ANOMALIES(
  MODEL `anomaly_detection.app_trading_volume_model_from_unaggregated`,
  STRUCT(0.95 AS anomaly_prob_threshold),
  TABLE new_data
);

--------------- With custom threshold ------------------
WITH new_data AS (
  SELECT
    TIMESTAMP("2025-10-01 00:00:00 UTC") AS Datetime,
    200 AS Volume,
    EXTRACT(HOUR FROM TIMESTAMP("2025-10-01 00:00:00 UTC")) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM TIMESTAMP("2025-10-01 00:00:00 UTC")) AS day_of_week
)

SELECT
  Datetime,
  Volume,
  anomaly_probability,
  lower_bound,
  upper_bound,
  CASE
    WHEN Volume < lower_bound * 0.95 OR Volume > upper_bound * 1.05 THEN TRUE
    ELSE FALSE
  END AS is_custom_anomaly
FROM ML.DETECT_ANOMALIES(
  MODEL `anomaly_detection.app_trading_volume_model_from_unaggregated`,
  STRUCT(0.95 AS anomaly_prob_threshold),
  TABLE new_data
);


-- Even 200 gets flagged as anomaly, that's totally expected because of Floating-point precision, 
-- even if we use 200, internally it might be stored as something like 199.99999999999997.

--------------------- Absolute volume delta threshold Threashold ----------------
-- lower_bound / upper_bound → model-predicted confidence interval
-- (lower_bound + upper_bound)/2 → predicted mean
-- ABS(Volume - mean) > 50 → custom anomaly rule
-- is_custom_anomaly → flags the row if it exceeds defined threshold

-- Replace the timestamp and volume below with the new hour
WITH new_data AS (
  SELECT
    TIMESTAMP("2025-10-01 00:00:00 UTC") AS Datetime,
    600 AS Volume,
    EXTRACT(HOUR FROM TIMESTAMP("2025-10-01 00:00:00 UTC")) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM TIMESTAMP("2025-10-01 00:00:00 UTC")) AS day_of_week
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
  TABLE new_data
);
