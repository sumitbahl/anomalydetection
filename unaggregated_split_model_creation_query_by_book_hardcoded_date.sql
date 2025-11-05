-- Step 1: Train only on data before Sept 26
CREATE OR REPLACE MODEL `anomaly_detection.app_trading_volume_model_by_book_split_and_eval`
OPTIONS(
  MODEL_TYPE = 'ARIMA_PLUS_XREG',
  TIME_SERIES_TIMESTAMP_COL = 'Datetime',
  TIME_SERIES_DATA_COL = 'Volume',
  TIME_SERIES_ID_COL = 'BookId',
  HOLIDAY_REGION = "US"
) AS
SELECT
  TIMESTAMP_TRUNC(Datetime_UTC, HOUR) AS Datetime,
  BookId,
  COUNT(*) AS Volume,
  EXTRACT(HOUR FROM Datetime_UTC) AS hour_of_day,
  EXTRACT(DAYOFWEEK FROM Datetime_UTC) AS day_of_week
FROM `anomaly_detection.book_volume_unaggregated`
WHERE Datetime_UTC < '2025-09-26'
GROUP BY Datetime, BookId, hour_of_day, day_of_week;
