#spark-submit --master yarn --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,org.apache.hbase:hbase-client:2.4.0,org.apache.hbase:hbase-common:2.4.0 kafka_hbase_writer_weather.py



import json
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
HBASE_ZK_QUORUM = "ip-172-31-91-77.ec2.internal"
HBASE_ZK_PORT = "2181"
HBASE_TABLE_NAME = "zhehao_hbase_real_weather"
HBASE_COLUMN_FAMILY = "weather_data"

NAMENODE_ADDRESS = "ip-172-31-91-77.ec2.internal:8020"
CHECKPOINT_LOCATION = f"hdfs://{NAMENODE_ADDRESS}/user/zhehao/spark_checkpoints/real_weather_stream"

KAFKA_BOOTSTRAP = "ip-172-31-91-77.ec2.internal:9092"
KAFKA_TOPIC = "zhehao_weather_reports_realtime"

# Data Schema
data_schema = StructType([
    StructField("datetime_ept", StringType(), True), 
    StructField("temp", DoubleType(), True),
    StructField("prcp", DoubleType(), True),
    StructField("hmdt", DoubleType(), True),
    StructField("wnd_spd", DoubleType(), True),
    StructField("atm_press", DoubleType(), True)
])


def foreach_batch_writer(batch_df, batch_id):
    """
    Write batch directly to HBase from driver
    """
    logger.info(f"=" * 60)
    logger.info(f"Processing batch {batch_id}")
    
    if batch_df.rdd.isEmpty():
        logger.info(f"Batch {batch_id} is empty")
        return
    
    # Collect data to driver
    rows = batch_df.collect()
    logger.info(f"Batch {batch_id} contains {len(rows)} records")
    
    # Print sample data
    for i, row in enumerate(rows[:3]):
        logger.info(f"Sample row {i}: datetime={row.datetime_ept}, temp={row.temp}, prcp={row.prcp}")
    
    # Import Java classes
    from py4j.java_gateway import java_import
    spark = SparkSession.getActiveSession()
    sc = spark.sparkContext
    gw = sc._gateway
    
    java_import(gw.jvm, "org.apache.hadoop.hbase.*")
    java_import(gw.jvm, "org.apache.hadoop.hbase.client.*")
    java_import(gw.jvm, "org.apache.hadoop.hbase.util.*")
    
    HBaseConfiguration = gw.jvm.org.apache.hadoop.hbase.HBaseConfiguration
    TableName = gw.jvm.org.apache.hadoop.hbase.TableName
    ConnectionFactory = gw.jvm.org.apache.hadoop.hbase.client.ConnectionFactory
    Put = gw.jvm.org.apache.hadoop.hbase.client.Put
    Bytes = gw.jvm.org.apache.hadoop.hbase.util.Bytes
    
    # Create HBase connection
    conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", HBASE_ZK_QUORUM)
    conf.set("hbase.zookeeper.property.clientPort", HBASE_ZK_PORT)
    
    logger.info(f"Connecting to HBase: {HBASE_ZK_QUORUM}:{HBASE_ZK_PORT}")
    
    connection = None
    table = None
    success_count = 0
    
    try:
        connection = ConnectionFactory.createConnection(conf)
        table_name = TableName.valueOf(HBASE_TABLE_NAME)
        table = connection.getTable(table_name)
        
        logger.info(f"Connected to HBase table: {HBASE_TABLE_NAME}")
        
        # Helper function to add data to Put object
        def add_to_put(put_obj, qualifier, value):
            if value is not None:
                put_obj.addColumn(
                    Bytes.toBytes(HBASE_COLUMN_FAMILY),
                    Bytes.toBytes(qualifier),
                    Bytes.toBytes(str(value))
                )

        for row in rows:
            try:
                datetime_ept_str = row.datetime_ept
                temp = row.temp
                prcp = row.prcp
                hmdt = row.hmdt
                wnd_spd = row.wnd_spd
                atm_press = row.atm_press
                
                if not datetime_ept_str:
                    logger.warning("Skipping row with empty datetime_ept")
                    continue
                
                # Use datetime_ept string as row key
                row_key = datetime_ept_str
                
                # Create Put
                put = Put(Bytes.toBytes(row_key))
                logger.info(f"Creating row with key: {row_key}")
                
                # Add all fields
                add_to_put(put, "datetime_ept", datetime_ept_str)
                add_to_put(put, "temp", temp)
                add_to_put(put, "prcp", prcp)
                add_to_put(put, "hmdt", hmdt)
                add_to_put(put, "wnd_spd", wnd_spd)
                add_to_put(put, "atm_press", atm_press)

                # Write to HBase
                table.put(put)
                success_count += 1
                logger.info(f"✅ Wrote row_key={row_key}, temp={temp}")
                
            except Exception as e:
                logger.error(f"❌ Failed to write row: {e}")
                import traceback
                traceback.print_exc()
        
        logger.info(f"Batch {batch_id} complete: wrote {success_count}/{len(rows)} records")
        
    except Exception as e:
        logger.error(f"HBase connection error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if table:
            table.close()
        if connection:
            connection.close()
    
    logger.info(f"=" * 60)


if __name__ == "__main__":
    
    spark = (
        SparkSession.builder
        .appName("KafkaToHBaseStream_WeatherReports")
        .config("spark.streaming.kafka.maxRatePerPartition", "100")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("=" * 60)
    logger.info("Kafka to HBase Streaming - Weather Reports")
    logger.info("=" * 60)
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP} / {KAFKA_TOPIC}")
    logger.info(f"HBase: {HBASE_ZK_QUORUM}:{HBASE_ZK_PORT} / {HBASE_TABLE_NAME}")
    logger.info(f"Checkpoint: {CHECKPOINT_LOCATION}")
    logger.info("=" * 60)
    
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", "100")
        .load()
    )
    
    # Parse JSON and extract fields
    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS json_string")
        .withColumn("value_json", from_json(col("json_string"), data_schema))
        .select(
            col("value_json.datetime_ept").alias("datetime_ept"),
            col("value_json.temp").alias("temp"),
            col("value_json.prcp").alias("prcp"),
            col("value_json.hmdt").alias("hmdt"),
            col("value_json.wnd_spd").alias("wnd_spd"),
            col("value_json.atm_press").alias("atm_press")
        )
        .filter(col("datetime_ept").isNotNull())
    )
    
    query = (
        parsed_df
        .writeStream
        .foreachBatch(foreach_batch_writer)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="10 seconds")
        .start()
    )
    
    logger.info("Streaming job started, waiting for data...")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping...")
        query.stop()
        spark.stop()