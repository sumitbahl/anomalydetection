WITH train_data AS (
  SELECT
    TIMESTAMP_TRUNC(Datetime_UTC, HOUR) AS Datetime,
    BookId,
    COUNT(*) AS Volume,
    EXTRACT(HOUR FROM Datetime_UTC) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM Datetime_UTC) AS day_of_week
  FROM `anomaly_detection.book_volume_unaggregated`
  WHERE Datetime_UTC < '2025-09-26'
  GROUP BY Datetime, BookId, hour_of_day, day_of_week
),

forecast_data AS (
  SELECT *
  FROM ML.FORECAST(
    MODEL `anomaly_detection.app_trading_volume_model_by_book_split_and_eval`,
    STRUCT(
      120 AS horizon,           -- 5 days × 24 hours
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
  FROM `anomaly_detection.book_volume_unaggregated`
  WHERE Datetime_UTC >= '2025-09-26'
  GROUP BY Datetime, BookId
)

SELECT
  f.BookId,
  SQRT(AVG(POW(f.forecast_value - a.Volume, 2))) AS rmse,
  AVG(ABS(f.forecast_value - a.Volume)) AS mae
FROM forecast_data f
JOIN actuals a
  ON f.BookId = a.BookId AND f.forecast_timestamp = a.Datetime
GROUP BY f.BookId
ORDER BY f.BookId;


-- Row	BookId	rmse	mae
-- 1	240540	7.1273256989367969e-13	1.7881992183295833e-13
-- 2	240541	0.51717923232230667	0.43084882928828633
-- 3	240542	2.5825727904075462e-13	6.2054065589715423e-14
-- 4	240543	4.2025448404623122e-12	1.0961305936992476e-12
-- 5	240544	0.0913776897020477	0.075626212431030612

-- Observaions

-- BookId	RMSE	MAE	Notes
-- 240540	7.1e-13	1.8e-13	Effectively zero error → model predicts perfectly
-- 240541	0.52	0.43	Small error → some deviation, probably the spike to 300 you injected
-- 240542	2.6e-13	6.2e-14	Effectively zero error → model predicts perfectly
-- 240543	4.2e-12	1.1e-12	Effectively zero error → model predicts perfectly
-- 240544	0.09	0.076	Tiny error → mostly correct, slight deviation for the spike to 600