// Purpose: Extract cyclical features crucial for capturing human behavior and seasonal trends.
spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW zhehao_features_time AS
SELECT
    *,
    -- The time of day is the most important periodic feature for load prediction
    HOUR(ts) AS hour_of_day,

    -- Captures the weekly cycle (weekday vs. weekend)
    DAYOFWEEK(ts) AS day_of_week, 

    -- Captures the yearly cycle and seasonal demand changes (e.g., summer heat)
    MONTH(ts) AS month_of_year,

    -- Captures long-term trend (load growth over years)
    YEAR(ts) AS year_val,

    -- Simplified boolean flag for weekend load profile
    CASE WHEN DAYOFWEEK(ts) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend
FROM zhehao_joined_raw;
""")