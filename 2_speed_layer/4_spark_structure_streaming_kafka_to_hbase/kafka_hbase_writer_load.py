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
HBASE_TABLE_NAME = "zhehao_hbase_real_load"
HBASE_COLUMN_FAMILY = "load_data"

NAMENODE_ADDRESS = "ip-172-31-91-77.ec2.internal:8020"
CHECKPOINT_LOCATION = f"hdfs://{NAMENODE_ADDRESS}/user/zhehao/spark_checkpoints/real_load_stream"

KAFKA_BOOTSTRAP = "ip-172-31-91-77.ec2.internal:9092"
KAFKA_TOPIC = "zhehao_power_readings_raw"

# Updated schema to match producer
data_schema = StructType([
    StructField("timestamp", StringType(), True),  # String format: "2025-12-02 15:00:00"
    StructField("current_reading", DoubleType(), True)
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
        logger.info(f"Sample row {i}: timestamp={row.timestamp}, reading={row.current_reading}")
    
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
        
        for row in rows:
            try:
                timestamp_str = row.timestamp
                current_reading = row.current_reading
                
                if not timestamp_str:
                    logger.warning("Skipping row with empty timestamp")
                    continue
                
                # Use timestamp string directly as row key
                # This is more human-readable
                row_key = timestamp_str
                
                # Also calculate milliseconds for storage
                from datetime import datetime
                try:
                    # Parse "2025-12-02 15:00:00" format
                    dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                    ts_ms = int(dt.timestamp() * 1000)
                except Exception as e:
                    logger.warning(f"Failed to parse timestamp: {timestamp_str}, error: {e}")
                    ts_ms = int(time.time() * 1000)
                
                # Create Put
                put = Put(Bytes.toBytes(row_key))
                logger.info(f"Creating row with key: {row_key}")
                
                # Add timestamp milliseconds (for reference)
                put.addColumn(
                    Bytes.toBytes(HBASE_COLUMN_FAMILY),
                    Bytes.toBytes("timestamp_ms"),
                    Bytes.toBytes(str(ts_ms))
                )
                
                # Add current_reading
                if current_reading is not None:
                    put.addColumn(
                        Bytes.toBytes(HBASE_COLUMN_FAMILY),
                        Bytes.toBytes("current_reading"),
                        Bytes.toBytes(str(current_reading))
                    )
                
                # Write to HBase
                table.put(put)
                success_count += 1
                logger.info(f"✅ Wrote row_key={row_key}, reading={current_reading}")
                
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
        .appName("KafkaToHBaseStream_PowerReadings")
        .config("spark.streaming.kafka.maxRatePerPartition", "100")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("=" * 60)
    logger.info("Kafka to HBase Streaming - Power Readings")
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
            col("value_json.timestamp").alias("timestamp"),
            col("value_json.current_reading").alias("current_reading")
        )
        .filter(col("timestamp").isNotNull())
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