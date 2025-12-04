package edu.uchicago.bigdata;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.FileReader;
import java.io.IOException;

public class CsvToThriftConverter {

    // Load Data Parquet Schema (Avro Schema 定义了 Parquet 文件的结构)
    private static final Schema LOAD_SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\", \"name\":\"LoadDataParquet\", \"fields\":[" +
                    "  {\"name\":\"datetime_ept\", \"type\":\"string\"}," +
                    "  {\"name\":\"mw\", \"type\":\"double\"}" +
                    "]}"
    );

    // Weather Data Parquet Schema
    private static final Schema WEATHER_SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\", \"name\":\"WeatherDataParquet\", \"fields\":[" +
                    "  {\"name\":\"datetime_ept\", \"type\":\"string\"}," +
                    "  {\"name\":\"TEMP\", \"type\":\"double\"}," +
                    "  {\"name\":\"PRCP\", \"type\":\"double\"}," +
                    "  {\"name\":\"HMDT\", \"type\":\"double\"}," +
                    "  {\"name\":\"WND_SPD\", \"type\":\"double\"}," +
                    "  {\"name\":\"ATM_PRESS\", \"type\":\"double\"}" +
                    "]}"
    );

    public static void main(String[] args) {
        String loadCsvPath = "combined_load_data_ept_mw.csv";
        String weatherCsvPath = "processed_weather_data.csv";

        // ** 输出 Parquet 文件名 **
        String loadParquetPath = "load.parquet";
        String weatherParquetPath = "weather.parquet";

        // Parquet 写入需要 Hadoop Configuration
        Configuration conf = new Configuration();

        try {
            // 1. 转换并写入 Load Data Parquet 文件
            writeLoadDataToParquet(conf, loadCsvPath, loadParquetPath);

            // 2. 转换并写入 Weather Data Parquet 文件
            writeWeatherDataToParquet(conf, weatherCsvPath, weatherParquetPath);

            System.out.println("✅ 转换完成。Parquet 文件已保存：");
            System.out.println("   - Load Data: " + loadParquetPath);
            System.out.println("   - Weather Data: " + weatherParquetPath);

        } catch (Exception e) {
            System.err.println("转换过程中发生错误:");
            e.printStackTrace();
        }
    }

    private static void writeLoadDataToParquet(Configuration conf, String csvPath, String parquetPath) throws IOException {
        long count = 0;
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).setIgnoreEmptyLines(true).build();
        Path outPath = new Path(parquetPath);

        // 使用 ParquetWriter 写入数据
        try (
                FileReader reader = new FileReader(csvPath);
                CSVParser csvParser = new CSVParser(reader, format);
                ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outPath)
                        .withSchema(LOAD_SCHEMA)
                        .withConf(conf)
                        .withCompressionCodec(CompressionCodecName.SNAPPY) // 推荐使用 SNAPPY 压缩
                        .build()
        ) {
            for (CSVRecord csvRecord : csvParser) {
                // 将 CSV 行转换为 Avro GenericRecord
                GenericRecord record = new GenericData.Record(LOAD_SCHEMA);

                record.put("datetime_ept", csvRecord.get("datetime_ept"));
                record.put("mw", Double.parseDouble(csvRecord.get("mw")));

                writer.write(record);
                count++;
            }
            System.out.println("Load Data 记录数: " + count);
        }
    }

    private static void writeWeatherDataToParquet(Configuration conf, String csvPath, String parquetPath) throws IOException {
        long count = 0;
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).setIgnoreEmptyLines(true).build();
        Path outPath = new Path(parquetPath);

        try (
                FileReader reader = new FileReader(csvPath);
                CSVParser csvParser = new CSVParser(reader, format);
                ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outPath)
                        .withSchema(WEATHER_SCHEMA)
                        .withConf(conf)
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .build()
        ) {
            for (CSVRecord csvRecord : csvParser) {
                GenericRecord record = new GenericData.Record(WEATHER_SCHEMA);

                record.put("datetime_ept", csvRecord.get("datetime_ept"));
                record.put("TEMP", Double.parseDouble(csvRecord.get("TEMP")));
                record.put("PRCP", Double.parseDouble(csvRecord.get("PRCP")));
                record.put("HMDT", Double.parseDouble(csvRecord.get("HMDT")));
                record.put("WND_SPD", Double.parseDouble(csvRecord.get("WND_SPD")));
                record.put("ATM_PRESS", Double.parseDouble(csvRecord.get("ATM_PRESS")));

                writer.write(record);
                count++;
            }
            System.out.println("Weather Data 记录数: " + count);
        }
    }
}