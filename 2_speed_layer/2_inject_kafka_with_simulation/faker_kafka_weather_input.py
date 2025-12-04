import json
import time
from datetime import datetime, timedelta
import random
import pytz
import math
from confluent_kafka import Producer 

# --- Configuration ---
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'zhehao_weather_reports_realtime'
EASTERN_TZ = pytz.timezone('America/New_York')

# --- Simulation Time Setup ---
# 定义起始时间： 2025-12-02 15:00:00 EPT
START_TIME_STR = '2025-12-02 15:00:00'
INITIAL_TIME = EASTERN_TZ.localize(datetime.strptime(START_TIME_STR, '%Y-%m-%d %H:%M:%S'))

# 定义每个字段的最大随机波动范围 (Noise)
NOISE_RANGES = {
    "temp": 0.8,        # 温度：± 0.8 °C
    "prcp": 0.02,       # 降水：± 0.02 mm
    "hmdt": 3.0,        # 湿度：± 3.0 %
    "wnd_spd": 1.5,     # 风速：± 1.5 km/h
    "atm_press": 0.2    # 大气压：± 0.2 hPa
}

def apply_noise(value, field_name):
    """Adds a random offset within the defined NOISE_RANGES."""
    noise = NOISE_RANGES.get(field_name, 0)
    return value + random.uniform(-noise, noise)

def get_base_weather_data(dt: datetime):
    """
    Generates base weather metrics based on the hour of the day 
    to simulate a realistic daily cycle for a winter/spring environment.
    """
    hour = dt.hour
    
    # --- 1. Temperature (TEMP) ---
    # 使用余弦波模拟日温度周期：最高点在下午2-4点 (hour=14-16)，最低点在凌晨4-6点 (hour=4-6)
    # 假设冬季日平均温度在 5°C 左右，日振幅为 10°C
    # 调整小时数让波峰出现在下午 (例如 15点)
    temp_cycle = math.cos(math.radians((hour - 15) * 15)) # 周期为 24小时, 360/24=15
    base_temp = 5.0 + (5.0 * temp_cycle) # 基础温度 5°C, 振幅 5°C
    
    # --- 2. Humidity (HMDT) ---
    # 湿度与温度大致反相关，但有较高的基础值 (如 70%)
    base_hmdt = 70.0 + (10.0 * -temp_cycle) 
    
    # --- 3. Wind Speed (WND_SPD) ---
    # 风速波动较大，平均值 10 km/h
    base_wnd_spd = 8.0 + (3.0 * random.random()) # 8到11之间随机
    
    # --- 4. Atmospheric Pressure (ATM_PRESS) ---
    # 大气压波动极小，接近 1010 hPa
    base_atm_press = 1010.0 + (2.0 * math.sin(math.radians(hour * 15))) # 轻微日变化
    
    # --- 5. Precipitation (PRCP) ---
    # 大多数时间为 0，以 5% 的概率出现轻微降水（0.1 - 0.5 mm）
    base_prcp = 0.0
    if random.random() < 0.05: # 5% 概率下雨
        base_prcp = random.uniform(0.1, 0.5)

    return {
        "temp": base_temp,
        "prcp": base_prcp,
        "hmdt": base_hmdt,
        "wnd_spd": base_wnd_spd,
        "atm_press": base_atm_press
    }

def produce_weather_reading_dynamic():
    """Generates weather data based on dynamic time-based trends and adds noise."""
    
    producer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'weather-producer-client-dynamic'
    }
    try:
        producer = Producer(producer_conf)
    except Exception as e:
        print(f"Error initializing Kafka Producer: {e}")
        return

    current_time = INITIAL_TIME
    
    while True:
        # 1. Get base data based on the hour
        base_data = get_base_weather_data(current_time)
        datetime_ept_str = current_time.strftime('%Y-%m-%d %H:%M:%S')

        # 2. Apply noise and bounds check
        temp_noisy = apply_noise(base_data["temp"], "temp")
        prcp_noisy = apply_noise(base_data["prcp"], "prcp")
        hmdt_noisy = apply_noise(base_data["hmdt"], "hmdt")
        wnd_spd_noisy = apply_noise(base_data["wnd_spd"], "wnd_spd")
        atm_press_noisy = apply_noise(base_data["atm_press"], "atm_press")
        
        # 3. Build final message (applying bounds)
        message = {
            "datetime_ept": datetime_ept_str,
            "temp": round(temp_noisy, 2),
            "prcp": round(max(0.0, prcp_noisy), 2),
            "hmdt": round(max(0.0, min(100.0, hmdt_noisy)), 2),
            "wnd_spd": round(max(0.0, wnd_spd_noisy), 2),
            "atm_press": round(atm_press_noisy, 2)
        }
        
        # 4. JSON serialization and Send to Kafka
        message_json = json.dumps(message).encode('utf-8')
        
        try:
            producer.produce(topic=TOPIC_NAME, value=message_json, key=datetime_ept_str.encode('utf-8'))
            producer.poll(0)
            
            print(f"Produced message: {message}")
            
        except Exception as e:
            print(f"Failed to produce message: {e}")
            
        
        # --- Time Step ---
        # Increment time by one hour for the next data point
        current_time += timedelta(hours=1)

        # Control simulation speed
        time.sleep(1) 

    producer.flush()

if __name__ == "__main__":
    produce_weather_reading_dynamic()