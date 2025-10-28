CREATE OR REPLACE MODEL `anomaly_detection.app_trading_volume_model_from_unaggregated`
OPTIONS(
  MODEL_TYPE = 'ARIMA_PLUS_XREG',
  time_series_timestamp_col = 'Datetime',
  time_series_data_col = 'Volume',
  HOLIDAY_REGION = "US"
) AS
WITH hourly AS (
  SELECT
    TIMESTAMP_TRUNC(Datetime_UTC, HOUR) AS Datetime,
    EXTRACT(HOUR FROM Datetime_UTC) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM Datetime_UTC) AS day_of_week
  FROM `anomaly_detection.unaggregated_volume`
  WHERE Datetime_UTC IS NOT NULL
)
SELECT
  Datetime,
  COUNT(*) AS Volume,
  hour_of_day,
  day_of_week
FROM hourly
GROUP BY Datetime, hour_of_day, day_of_week
ORDER BY Datetime;

