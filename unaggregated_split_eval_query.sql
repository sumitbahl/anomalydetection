WITH eval_data AS (
  SELECT
    TIMESTAMP_TRUNC(Datetime_UTC, HOUR) AS Datetime,
    COUNT(*) AS Volume,
    EXTRACT(HOUR FROM Datetime_UTC) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM Datetime_UTC) AS day_of_week
  FROM `anomaly_detection.unaggregated_volume`
  WHERE Datetime_UTC >= '2025-09-26'
  GROUP BY 1, 3, 4
)
SELECT
  *
FROM
  ML.EVALUATE(MODEL `anomaly_detection.app_trading_volume_model_train_split_and_eval`, TABLE eval_data);

--Metric	Use it?	Why
--MAE (6.7)	 Yes	Easy to interpret — average absolute error is small relative to 200 baseline (~3%)
--RMSE (51.6)	 Yes	Captures how large errors can get; highlights impact of anomalies
--MAPE (113%)	 No	Misleading when actual values spike or are small
--sMAPE (1.68)	 Maybe	Somewhat more stable than MAPE, but still skewed by anomalies
