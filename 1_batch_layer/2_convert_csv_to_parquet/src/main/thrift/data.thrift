// File: src/main/thrift/data.thrift

// ***** 更改了命名空间为 edu.uchicago.bigdata *****
namespace java edu.uchicago.bigdata 

// 负荷数据结构
struct LoadData {
  1: required string datetime_ept,
  2: required double mw
}

// 天气数据结构
struct WeatherData {
  1: required string datetime_ept,
  2: required double TEMP,
  3: required double PRCP,
  4: required double HMDT,
  5: required double WND_SPD,
  6: required double ATM_PRESS
}

// 容器结构
struct DataRecords {
  1: required list<LoadData> load_data,
  2: required list<WeatherData> weather_data
}