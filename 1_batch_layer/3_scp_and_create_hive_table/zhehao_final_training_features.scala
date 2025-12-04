// Purpose: Save the fully engineered feature set to a permanent, optimized Hive table (Parquet format)
// This is the output of the Batch Layer, ready for Machine Learning training (Spark MLlib).
spark.sql("""
CREATE TABLE zhehao_final_training_features 
STORED AS PARQUET AS
SELECT * FROM zhehao_final_features;
""")