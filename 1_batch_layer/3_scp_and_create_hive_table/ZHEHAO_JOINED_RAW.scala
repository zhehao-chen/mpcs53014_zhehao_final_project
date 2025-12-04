// Purpose: Join load and weather data on the timestamp, and cast the datetime string to a TIMESTAMP type.
spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW zhehao_joined_raw AS
SELECT
    l.datetime_ept,
    CAST(l.datetime_ept AS TIMESTAMP) AS ts,  -- Convert string to TIMESTAMP for chronological ordering and function usage
    l.mw,                                     -- Target Variable (Load)
    w.TEMP,
    w.PRCP,
    w.HMDT,
    w.WND_SPD,
    w.ATM_PRESS
FROM zhehao_load_data l
LEFT JOIN zhehao_weather_data w
    ON l.datetime_ept = w.datetime_ept
ORDER BY ts; 
""")

// Verify the join operation
spark.sql("SELECT * FROM zhehao_joined_raw LIMIT 5").show()