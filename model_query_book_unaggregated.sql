-- 5 Book IDs
-- Different normal volumes:
-- Book 1 → 100 per hour
-- Book 2 → 200 per hour
-- Book 3 → 300 per hour
-- Book 4 → 400 per hour
-- Book 5 → 500 per hour

-- Injected anomalies:
-- Book 2 → 300 spike
-- Book 5 → 600 spike


CREATE OR REPLACE MODEL `anomaly_detection.app_trading_volume_model_by_book`
OPTIONS(
  MODEL_TYPE = 'ARIMA_PLUS_XREG',
  TIME_SERIES_TIMESTAMP_COL = 'Datetime',
  TIME_SERIES_DATA_COL = 'Volume',
  TIME_SERIES_ID_COL = 'BookId',
  HOLIDAY_REGION = "US"
) AS
WITH hourly AS (
  SELECT
    TIMESTAMP_TRUNC(Datetime_UTC, HOUR) AS Datetime,
    BookId,
    EXTRACT(HOUR FROM Datetime_UTC) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM Datetime_UTC) AS day_of_week
  FROM `anomaly_detection.book_volume_unaggregated`
  WHERE Datetime_UTC IS NOT NULL
)
SELECT
  Datetime,
  BookId,
  COUNT(*) AS Volume,
  hour_of_day,
  day_of_week
FROM hourly
GROUP BY Datetime, BookId, hour_of_day, day_of_week
ORDER BY Datetime, BookId;
