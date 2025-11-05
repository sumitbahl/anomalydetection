-- Step 1: Dynamically determine training data (all except last 5 days)
CREATE OR REPLACE MODEL `anomaly_detection.app_trading_volume_model_by_book_split_and_eval`
OPTIONS(
  MODEL_TYPE = 'ARIMA_PLUS_XREG',
  TIME_SERIES_TIMESTAMP_COL = 'Datetime',
  TIME_SERIES_DATA_COL = 'Volume',
  TIME_SERIES_ID_COL = 'BookId',
  HOLIDAY_REGION = "US"
) AS

WITH last_timestamp AS (
  SELECT MAX(Datetime_UTC) AS max_ts
  FROM `anomaly_detection.book_volume_unaggregated`
),
train_data AS (
  SELECT
    TIMESTAMP_TRUNC(Datetime_UTC, HOUR) AS Datetime,
    BookId,
    COUNT(*) AS Volume,
    EXTRACT(HOUR FROM Datetime_UTC) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM Datetime_UTC) AS day_of_week
  FROM `anomaly_detection.book_volume_unaggregated`, last_timestamp
  WHERE Datetime_UTC < TIMESTAMP_SUB(max_ts, INTERVAL 5 DAY)
  GROUP BY Datetime, BookId, hour_of_day, day_of_week
)
SELECT *
FROM train_data;
