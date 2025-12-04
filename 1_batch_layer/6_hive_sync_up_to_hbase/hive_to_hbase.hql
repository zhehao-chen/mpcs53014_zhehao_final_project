CREATE EXTERNAL TABLE IF NOT EXISTS zhehao_hive_on_hbase_predictions (
    row_key STRING,
    prediction_value DOUBLE,
    prediction_ts BIGINT
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,data:value,data:timestamp"
)
TBLPROPERTIES (
    "hbase.table.name" = "zhehao_daily_predictions_hbase",
    "hbase.mapred.output.outputtable" = "zhehao_daily_predictions_hbase"
);

INSERT OVERWRITE TABLE zhehao_hive_on_hbase_predictions
-- 1. 选取源表 load_forecast.zhehao_daily_predictions 中最新的 24 条记录
SELECT 
    t.ts,                -- 映射到 HBase Row Key (STRING)
    t.predicted_load_mw, 
    UNIX_TIMESTAMP(t.ts) -- 映射到 HBase 列 data:timestamp (BIGINT)
FROM 
    load_forecast.zhehao_daily_predictions t
ORDER BY 
    t.ts DESC;

INSERT OVERWRITE TABLE zhehao_hive_on_hbase_predictions
SELECT 
    t.ts,                
    t.predicted_load_mw, 
    UNIX_TIMESTAMP(t.ts) 
FROM 
    load_forecast.zhehao_daily_predictions t
ORDER BY 
    t.ts DESC           
LIMIT 24;                