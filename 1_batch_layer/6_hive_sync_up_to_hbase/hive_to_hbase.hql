CREATE EXTERNAL TABLE zhehao_hive_on_hbase_predictions (
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


---
-- Load the latest 24 records into the Hive-on-HBase table using the second (corrected) INSERT statement.
-- This section correctly uses LIMIT 24 to select only the most recent predictions.
-- put this into a cronjob in real production and run once 24 hours
---

SET hive.exec.dynamic.partition.mode=nonstrict; 

INSERT INTO TABLE zhehao_hive_on_hbase_predictions
SELECT 
    -- RowKey: 
    CAST(t.ts AS STRING) AS row_key,
    
    -- prediction_value: 
    t.predicted_load_mw AS prediction_value,
    
    -- prediction_ts: Unix 
    UNIX_TIMESTAMP(t.ts) AS prediction_ts
FROM 
    load_forecast.zhehao_daily_predictions t
ORDER BY 
    t.ts DESC
LIMIT 24;
