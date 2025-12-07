import json
import time
from datetime import datetime, timedelta
import random
import pytz

# Install necessary libraries: pip install confluent-kafka pytz
from confluent_kafka import Producer 

# --- Configuration ---
KAFKA_BROKER = 'ip-172-31-91-77.ec2.internal:9092' 
TOPIC_NAME = 'zhehao_power_readings_raw'
EASTERN_TZ = pytz.timezone('America/New_York')

# --- Simulation Time Setup ---
START_TIME_STR = '2025-12-01 00:00:00'
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
        hour = now_ept.hour 
        
        # 2. Simulate MW load data 
        
        if 0 <= hour <= 5: 
            # Night time low load
            base_load_raw = 8500.0 + (hour * 50) - (random.uniform(0, 300) if hour < 3 else 0)
        elif 6 <= hour <= 8:
            # Morning ramp-up
            base_load_raw = 8800.0 + ((hour - 5) * 600) 
        elif 9 <= hour <= 17:
            # Day time/Afternoon peak plateau
            midday_adjustment = 0
            if hour in [11, 13]:
                midday_adjustment = -200
            elif hour >= 14:
                midday_adjustment = (hour - 13) * 150 
            
            base_load_raw = 10000.0 + midday_adjustment
        elif 18 <= hour <= 23:
            # Evening and late night decline
            base_load_raw = 10800.0 - ((hour - 18) * 300)
        else:
            base_load_raw = 8500.0 

        # Add random fluctuation
        load_mw = base_load_raw + random.uniform(-150.0, 150.0)
        
        # 3. Build message body
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
            
        
        # --- Time step ---
        # Increment time by one hour for the next loop
        current_time += timedelta(hours=1)

        # Simulate data stream speed (sending one hour data point every 1 second)
        time.sleep(1) 

    producer.flush()

if __name__ == "__main__":
    produce_power_reading_hourly()