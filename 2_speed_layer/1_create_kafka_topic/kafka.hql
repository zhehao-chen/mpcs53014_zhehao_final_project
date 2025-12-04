kafka-topics.sh --create --topic zhehao_power_readings_raw --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

kafka-topics.sh --create --topic zhehao_weather_reports_realtime --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1