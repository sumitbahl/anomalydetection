SELECT *
FROM ML.DETECT_ANOMALIES(
  MODEL `anomaly_detection.app_trading_volume_model_by_book`,
  STRUCT(0.999 AS anomaly_prob_threshold)
)
WHERE is_anomaly = TRUE
AND ABS(Volume - (lower_bound + upper_bound)/2) > 5
ORDER BY Datetime DESC, BookId