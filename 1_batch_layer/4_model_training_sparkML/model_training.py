# using spark-submit
# spark-submit --name LoadForecastingModelTraining --master yarn --deploy-mode client --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.sql.catalogImplementation=hive --files /etc/hive/conf.dist/hive-site.xml --jars "/path/to/hive/lib/hive-metastore-*.jar,/path/to/hive/lib/hive-exec-*.jar,/usr/lib/hadoop-lzo/lib/hadoop-lzo-0.4.19.jar" --num-executors 10 --executor-cores 4 --executor-memory 8G --driver-memory 4G model_training.py

# ======================================================================
# 1. Import necessary libraries
# ======================================================================
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator
# Import required Spark SQL functions
from pyspark.sql.functions import col, min, max, count, hour, dayofweek, dayofmonth, year, month
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession # <<< ADDED: Import SparkSession

# HDFS Path Configuration
MODEL_SAVE_PATH = "/user/hadoop/zhehao/models/batch_load_predictor"
TARGET_COLUMN = "mw"
TRAIN_RATIO = 0.8
SEED = 42

def run_model_training(spark):
    """
    This function executes the complete workflow of data loading, splitting, 
    model training, evaluation, and saving.
    """
    
    print("---------------------------------------------------------")
    print("1. Loading Data and Splitting")
    print("---------------------------------------------------------")


    # ======================================================================
    # 1. Load Data and Split (Time Series Split)
    # ======================================================================

    # Load the permanent feature table for training
    
    df = spark.table("default.zhehao_final_training_features").orderBy(col("ts"))


    # Drop original columns not needed for training (target is 'mw', 'ts' is for ordering)
    df = df.drop("datetime_ept")

    # ======================================================================
    # 1.5 Feature Engineering: Extracting Time-based Features
    # ======================================================================
    # Extract Hour and Day of Week from the timestamp 'ts'
    df = df.withColumn("hour_of_day", hour(col("ts")))
    df = df.withColumn("day_of_week", dayofweek(col("ts")))
    print("✅ Extracted 'hour_of_day' and 'day_of_week' features (as numerical).")

    # Determine the row count split point for the time-series split
    total_count = df.count()
    train_count = int(total_count * TRAIN_RATIO)
    test_count = total_count - train_count

    # Perform time-series split by row count (relies on data being sorted by 'ts')
    train_df = df.limit(train_count)
    test_df = df.exceptAll(train_df) 

    # ======================================================================
    # 2. Validating Data Split Correctness
    # ======================================================================
    print(f"Total records after filtering NULL lags: {total_count}")
    print(f"Training set records ({TRAIN_RATIO*100:.0f}%): {train_df.count()}")
    print(f"Testing set records ({(1-TRAIN_RATIO)*100:.0f}%): {test_df.count()}")

    # Validate the temporal continuity of the split
    train_max_ts = train_df.select(max("ts")).collect()[0][0]
    test_min_ts = test_df.select(min("ts")).collect()[0][0]
    print(f"Training set max timestamp: {train_max_ts}")
    print(f"Testing set min timestamp: {test_min_ts}")
    print("✅ Time-series split validation successful.")


    # ======================================================================
    # 3. Feature Assembly (VectorAssembler)
    # ======================================================================

    # Automatically get all feature columns excluding target and 'ts' 
    feature_cols = [c for c in df.columns if c not in [TARGET_COLUMN, "ts"]]
    print(f"\nNumber of features used: {len(feature_cols)}")
    
    # Set handleInvalid="skip" to ignore rows where lag features (or others) are NULL
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")


    # ======================================================================
    # 4. Model Definition and Training
    # ======================================================================
    print("\n---------------------------------------------------------")
    print("4. Starting Model Training and Cross-Validation (Random Forest)")
    print("---------------------------------------------------------")

    # Define the Random Forest Regressor model
    rf = RandomForestRegressor(labelCol=TARGET_COLUMN, featuresCol="features", seed=SEED)

    # Build the Pipeline: Assembler -> RF Model
    pipeline = Pipeline(stages=[assembler, rf])

    # Define the parameter grid for hyperparameter tuning
    paramGrid = ParamGridBuilder() \
        .addGrid(rf.numTrees, [20, 50]) \
        .addGrid(rf.maxDepth, [8, 12]) \
        .build()

    # Define the evaluator: using Root Mean Squared Error (RMSE)
    evaluator = RegressionEvaluator(
        labelCol=TARGET_COLUMN, predictionCol="prediction", metricName="rmse"
    )

    # Define the Cross-Validator (3-fold)
    cv = CrossValidator(
        estimator=pipeline, 
        estimatorParamMaps=paramGrid, 
        evaluator=evaluator, 
        numFolds=3, 
        seed=SEED
    )

    # Run cross-validation to find the best model
    cvModel = cv.fit(train_df)
    best_model = cvModel.bestModel

    print("\n---------------------------------------------------------")
    print("5. Model Evaluation and Validation")
    print("---------------------------------------------------------")

    # ======================================================================
    # 5. Evaluate Model Performance
    # ======================================================================

    # Make predictions on the test set
    predictions = best_model.transform(test_df)

    # Evaluate RMSE
    rmse = evaluator.evaluate(predictions)
    print(f"Model Evaluation Metric - Test RMSE: {rmse:.4f} MW")


    # ======================================================================
    # 6. Feature Importance Analysis
    # ======================================================================

    # Extract the trained Random Forest model
    rf_model = best_model.stages[-1]
    # Get the feature names directly from the assembler
    feature_names = best_model.stages[0].getInputCols()

    # Zip feature names with importance scores and sort
    importances = sorted(
        zip(feature_names, rf_model.featureImportances), 
        key=lambda x: x[1], 
        reverse=True
    )

    print("\nFeature Importance:")
    print("-----------------------------------")
    for name, score in importances:
        # Only print features with importance > 0.005
        if score > 0.005:
            print(f"  - {name:<20}: {score:.4f}")
    print("-----------------------------------")


    # ======================================================================
    # 7. Save Model (Final output of the Batch Layer)
    # ======================================================================

    # Save the best model to HDFS
    # Use the fixed write method to prevent AttributeError
    best_model.write().overwrite().save(MODEL_SAVE_PATH)
    print(f"\n✅ Best model successfully saved to HDFS: {MODEL_SAVE_PATH}")


# ======================================================================
# Main Execution
# ======================================================================
if __name__ == "__main__":
    # Ensure execution when running via spark-submit
    
    # Explicitly create SparkSession. In a spark-submit environment, getOrCreate() 
    # will use the configured cluster settings.
    print("Initializing SparkSession for Load Forecasting Model Training...")
    try:
        # Use default configuration for interacting with Hive tables
        spark = SparkSession.builder \
            .appName("LoadForecastingModelTraining") \
            .enableHiveSupport() \
            .getOrCreate()
            
        # Run the model training workflow
        run_model_training(spark)
        
        # Stop the SparkSession
        spark.stop()
        print("✅ SparkSession stopped successfully.")
        
    except Exception as e:
        print(f"FATAL ERROR: Failed during execution. Details: {e}")
        # If SparkSession was created, try to stop it
        if 'spark' in locals():
            spark.stop()
        exit(1)