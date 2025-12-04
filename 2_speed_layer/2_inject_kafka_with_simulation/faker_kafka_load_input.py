import json
import time
from datetime import datetime, timedelta
import random
import pytz

# Install necessary libraries: pip install confluent-kafka pytz
from confluent_kafka import Producer 

# --- Configuration ---
# Set the actual Broker IP address, consistent with the Spark script
KAFKA_BROKER = 'ip-172-31-91-77.ec2.internal:9092' 
TOPIC_NAME = 'zhehao_power_readings_raw'
EASTERN_TZ = pytz.timezone('America/New_York')

# --- Simulation Time Setup ---
# Start time defined as 2025-12-02 15:00:00 EPT
START_TIME_STR = '2025-12-02 15:00:00'
INITIAL_TIME = EASTERN_TZ.localize(datetime.strptime(START_TIME_STR, '%Y-%m-%d %H:%M:%S'))

def produce_power_reading_hourly():
    """Simulates generating hourly power readings and sending to Kafka"""
    
    producer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'power-producer-client-hourly'
    }
    try:
        producer = Producer(producer_conf)
    except Exception as e:
        print(f"Error initializing Kafka Producer: {e}")
        return

    current_time = INITIAL_TIME
    
    # Simulate continuous data generation
    while True:
        # 1. Use current simulated time
        now_ept = current_time
        datetime_ept_str = now_ept.strftime('%Y-%m-%d %H:%M:%S')

        # 2. Simulate MW load data 
        # Base load adjusted based on hour (simulating higher daytime load)
        base_load = 8000.0 + (current_time.hour * 100) 
        load_mw = base_load + random.uniform(-100.0, 100.0)
        
        # 3. Build message body (matches Spark Schema for RowKey and column names)
        message = {               
            "timestamp": datetime_ept_str,             
            "current_reading": round(load_mw, 2),                                 
        }
        
        # 4. JSON serialization
        message_json = json.dumps(message).encode('utf-8')
        
        # 5. Send message to Kafka
        try:
            # Asynchronous send
            producer.produce(
                topic=TOPIC_NAME, 
                value=message_json,
                key=datetime_ept_str.encode('utf-8') 
            )
            producer.poll(0)
            
            print(f"Produced message: {message}")
            
        except Exception as e:
            print(f"Failed to produce message: {e}")
            
        
        # --- Time step (Core modification) ---
        # Increment time by one hour for the next loop
        current_time += timedelta(hours=1)

        # Simulate data stream speed (sending one hour data point every 1 second)
        time.sleep(1) 

    producer.flush()

if __name__ == "__main__":
    # Assuming your Kafka Broker is running
    produce_power_reading_hourly()