// Purpose: Use Window Functions to generate autoregressive (load) and dynamic (weather change) features.
spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW zhehao_final_features AS
WITH WindowedFeatures AS (
    SELECT
        *,
        -- Short-term Autoregression: Load at t-1 is the single best predictor for load at t.
        LAG(mw, 1) OVER (ORDER BY ts) AS load_lag_1h,
        
        -- Daily Cycle: Yesterday's load at the same time is highly predictive.
        LAG(mw, 24) OVER (ORDER BY ts) AS load_lag_24h,
        
        -- Weekly Cycle: Last week's load at the same time is critical for weekly patterns.
        LAG(mw, 168) OVER (ORDER BY ts) AS load_lag_7d, 

        -- Lagged Weather: Accounts for the delay between a temperature change and a change in HVAC use.
        LAG(TEMP, 2) OVER (ORDER BY ts) AS temp_lag_2h,
        
        -- Temperature Change: Captures the effect of sudden temperature drops/rises (e.g., cold front).
        TEMP - LAG(TEMP, 24) OVER (ORDER BY ts) AS temp_change_24h,

        -- Moving Average: Captures a smoothed weekly trend, reducing noise from short-term spikes.
        AVG(mw) OVER (ORDER BY ts ROWS BETWEEN 167 PRECEDING AND CURRENT ROW) AS load_ma_7d

    FROM zhehao_features_weather
)
SELECT * FROM WindowedFeatures
-- Filter out the first 7 days (168 hours) of data, as they cannot have complete lag features (load_lag_7d will be NULL).
WHERE load_lag_7d IS NOT NULL 
ORDER BY ts;
""")