-- Step 1: Dynamically train on all but last 5 days and evaluate on last 5 days
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
),
forecast_data AS (
  SELECT *
  FROM ML.FORECAST(
    MODEL `anomaly_detection.app_trading_volume_model_by_book_split_and_eval`,
    STRUCT(
      120 AS horizon,           -- last 5 days * 24 hours
      0.999 AS confidence_level
    ),
    TABLE train_data
  )
),

actuals AS (
  SELECT
    TIMESTAMP_TRUNC(Datetime_UTC, HOUR) AS Datetime,
    BookId,
    COUNT(*) AS Volume
  FROM `anomaly_detection.book_volume_unaggregated`, last_timestamp
  WHERE Datetime_UTC >= TIMESTAMP_SUB(max_ts, INTERVAL 5 DAY)
  GROUP BY Datetime, BookId
)

select f.forecast_timestamp, f.bookId, f.forecast_value, a.volume,a.bookId, a.Datetime from forecast_data f inner join actuals a
  ON f.BookId = a.BookId
  AND f.forecast_timestamp = a.Datetime
