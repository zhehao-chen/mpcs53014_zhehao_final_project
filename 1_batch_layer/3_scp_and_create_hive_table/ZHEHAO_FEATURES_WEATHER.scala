// Purpose: Create features to account for the non-linear relationship between weather and load.
spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW zhehao_features_weather AS
SELECT
    *,
    -- Non-linear Feature: Load typically has a U-shaped relationship with TEMP (high load for extreme hot/cold).
    -- TEMP^2 allows linear models to capture this non-linearity.
    POWER(TEMP, 2) AS temp_squared,
    
    -- Precipitation Flag: Indicates periods where load might be affected by lighting or people staying indoors.
    CASE WHEN PRCP > 0 THEN 1 ELSE 0 END AS is_raining,
    
    -- Interaction Term: Captures the combined effect of high humidity and high temperature (perceived wet heat).
    HMDT * TEMP AS humidity_temp_interaction
    
FROM zhehao_features_time;
""")