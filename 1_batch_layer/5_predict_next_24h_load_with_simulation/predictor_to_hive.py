# -*- coding: utf-8 -*-
# ======================================================================
# Batch Model Prediction Script
# Loads a trained Random Forest model and generates future 24-hour electric load forecasts.
# Output columns are restricted to 'ts' and 'predicted_load_mw'.
# NEW: Predicted load field is rounded to two decimal places.
# ======================================================================
# using spark-submit
# spark-submit --name LoadForecastingBatchPrediction --master yarn --deploy-mode client --conf spark.sql.catalogImplementation=hive --files /etc/hive/conf.dist/hive-site.xml --jars /usr/lib/hive/lib/hive-metastore-3.1.3-amzn-20.jar,/usr/lib/hive/lib/hive-exec-3.1.3-amzn-20.jar,/usr/lib/hadoop-lzo/lib/hadoop-lzo-0.4.19.jar --num-executors 5 --executor-cores 2 --executor-memory 4G --driver-memory 2G predictor_to_hive.py

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.types import TimestampType, DoubleType, IntegerType
# FIX: Renaming PySpark's round function to spark_round to avoid conflict with Python's built-in round()
from pyspark.sql.functions import col, hour, dayofweek, lit, month, year, when, round as spark_round
from datetime import datetime, timedelta 
from faker import Faker 

# HDFS Path Configuration (Must match the training script)
MODEL_SAVE_PATH = "/user/hadoop/zhehao/models/batch_load_predictor"

def run_prediction(spark):
    """
    Executes the complete workflow of model loading and batch prediction for the next 24 hours load.
    """
    
    fake = Faker('en_US')

    print("---------------------------------------------------------")
    print("1. Loading Model")
    print("---------------------------------------------------------")

    try:
        best_model = PipelineModel.load(MODEL_SAVE_PATH)
        print(f"✅ PipelineModel successfully loaded from: {MODEL_SAVE_PATH}")
    except Exception as e:
        print(f"FATAL ERROR: Failed to load model from {MODEL_SAVE_PATH}. Ensure the path is correct and the model exists.")
        print(f"Details: {e}")
        return

    # ======================================================================
    # 2. Input Data Generation for Next 24 Hours
    # ======================================================================
    
    # Assuming current time is 2025-12-01 23:00:00, we predict 24 hours starting from 2025-12-02 00:00:00
    start_prediction_ts = datetime(2025, 12, 1, 0, 0, 0)
    
    BASE_LOAD = 9500.0
    BASE_TEMP = 15.0
    
    mock_data = []

    for i in range(24):
        current_ts = start_prediction_ts + timedelta(hours=i)
        
        load_offset = 0.0
        if i < 8:
            load_offset = -1000
        elif i < 18:
            load_offset = 500
        
        # These calls use the built-in Python round() function
        load_lag_1h = round(BASE_LOAD + load_offset + fake.pyfloat(min_value=-500, max_value=500, right_digits=2), 2)
        load_lag_24h = round(load_lag_1h * fake.pyfloat(min_value=0.9, max_value=1.1, right_digits=3), 2)
        load_lag_168h = round(load_lag_1h * fake.pyfloat(min_value=0.8, max_value=1.2, right_digits=3), 2)
        
        temp_variation = 0.0
        if 8 <= i <= 17:
            temp_variation = 5.0
            
        temp = round(BASE_TEMP + temp_variation + fake.pyfloat(min_value=-3, max_value=5, right_digits=1), 1)
        temp_lag_2h = round(temp + fake.pyfloat(min_value=-1, max_value=1, right_digits=1), 1)
        
        temp_change_24h = round(fake.pyfloat(min_value=-1.5, max_value=1.5, right_digits=1), 1) 
        load_ma_7d = round(BASE_LOAD + load_offset * 0.5 + fake.pyfloat(min_value=-300, max_value=300, right_digits=2), 2)
        
        temp = max(temp, 0.1) 
        temp_lag_2h = max(temp_lag_2h, 0.1) 

        temp_squared = round(temp * temp, 2)
        
        mock_data.append(
            (str(current_ts), 
             load_lag_1h, load_lag_24h, load_lag_168h, 
             temp, temp_lag_2h, temp_squared,
             temp_change_24h, load_ma_7d
            )
        )
    
    input_df = spark.createDataFrame(
        mock_data, 
        [
         "ts_str", 
         "load_lag_1h", "load_lag_24h", "load_lag_168h",
         "TEMP", "temp_lag_2h", "temp_squared",
         "temp_change_24h", "load_ma_7d"
        ]
    ).withColumn("ts", col("ts_str").cast(TimestampType())).drop("ts_str")


    print(f"\nInput Data Preview (Total rows: {input_df.count()}, Predicting next 24 hours):")
    input_df.printSchema()
    input_df.orderBy(col("ts").asc()).limit(5).show(truncate=False)

    
    # ======================================================================
    # 3. Feature Engineering Replication (Replicating and fixing missing features)
    # ======================================================================

    input_df = input_df.withColumn("PRCP", lit(0.0).cast(DoubleType()))
    input_df = input_df.withColumn("HMDT", lit(50.0).cast(DoubleType()))
    input_df = input_df.withColumn("WND_SPD", lit(10.0).cast(DoubleType()))
    input_df = input_df.withColumn("ATM_PRESS", lit(1013.0).cast(DoubleType()))

    input_df = input_df.withColumn("hour_of_day", hour(col("ts")))
    input_df = input_df.withColumn("day_of_week", dayofweek(col("ts")))
    input_df = input_df.withColumn("month_of_year", month(col("ts")))
    input_df = input_df.withColumn("year_val", year(col("ts")))

    input_df = input_df.withColumn(
        "is_weekend",
        when((col("day_of_week") == 1) | (col("day_of_week") == 7), 1).otherwise(0).cast(IntegerType())
    )

    input_df = input_df.withColumn(
        "is_raining",
        when(col("PRCP") > 0.0, 1).otherwise(0).cast(IntegerType())
    )
    
    input_df = input_df.withColumn(
        "humidity_temp_interaction",
        col("HMDT") * col("TEMP")
    )

    print("✅ Feature Engineering (All features including lag renaming and new features) successfully replicated.")


    # ======================================================================
    # 4. Model Prediction
    # ======================================================================

    predictions = best_model.transform(input_df)

    # ======================================================================
    # 5. Output Results
    # Key change: Selecting only 'ts' and 'predicted_load_mw', using spark_round to keep two decimal places.
    # ======================================================================
    
    output_df = predictions.select(
        "ts", 
        spark_round(col("prediction"), 2).alias("predicted_load_mw") # <--- Using spark_round
    ).orderBy("ts")

    print("\n---------------------------------------------------------")
    print(f"Prediction Results for Next 24 Hours ({start_prediction_ts} onwards)")
    print("---------------------------------------------------------")
    
    output_df.show(24, truncate=False) 

    # ======================================================================
    # 6. Save Results to Hive Table
    # ======================================================================
    # TODO: Please modify HIVE_TABLE_NAME according to your Hive database and table name
    HIVE_TABLE_NAME = "load_forecast.zhehao_daily_predictions" 
    
    print("\n---------------------------------------------------------")
    print(f"6. Saving Prediction Results to Hive Table: {HIVE_TABLE_NAME}")
    print("---------------------------------------------------------")

    try:
        # Write mode changed to 'append'
        output_df.write \
            .mode("append") \
            .saveAsTable(HIVE_TABLE_NAME)
            
        print(f"✅ Prediction results successfully saved (appended) to Hive table: {HIVE_TABLE_NAME}")
        
    except Exception as e:
        print(f"❌ ERROR: Failed to save results to Hive table {HIVE_TABLE_NAME}.")
        print(f"Details: {e}")


# ======================================================================
# Main Execution
# ======================================================================
if __name__ == "__main__":
    
    print("Initializing SparkSession for Batch Prediction...")
    try:
        spark = SparkSession.builder \
            .appName("LoadForecastingBatchPrediction") \
            .enableHiveSupport() \
            .getOrCreate()
            
        run_prediction(spark)
        
        spark.stop()
        print("✅ SparkSession stopped successfully.")
        
    except Exception as e:
        print(f"FATAL ERROR: Failed during execution. Details: {e}")
        if 'spark' in locals():
            spark.stop()
        exit(1)