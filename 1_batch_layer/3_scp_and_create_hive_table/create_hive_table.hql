CREATE EXTERNAL TABLE IF NOT EXISTS zhehao_weather_data (
    datetime_ept    STRING,                 -- 原始时间戳字符串 (在 Spark 中转换为 TIMESTAMP 更灵活)
    temp            DOUBLE,                 -- 温度
    prcp            DOUBLE,                 -- 降水量
    hmdt            DOUBLE,                 -- 湿度
    wnd_spd         DOUBLE,                 -- 风速
    atm_press       DOUBLE                  -- 大气压力
)
STORED AS PARQUET                       -- 确认使用 Parquet 格式以获得最佳性能
LOCATION '/user/hadoop/zhehao/weather_data/';

CREATE EXTERNAL TABLE IF NOT EXISTS zhehao_load_data (
    datetime_ept    STRING,                 
    mw              DOUBLE
)
STORED AS PARQUET                       
LOCATION '/user/hadoop/zhehao/load_data/';
