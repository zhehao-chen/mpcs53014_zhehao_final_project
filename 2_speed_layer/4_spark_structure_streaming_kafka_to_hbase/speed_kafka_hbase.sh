#!/bin/bash

spark-submit   --master yarn   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.hbase:hbase-client:2.4.0,org.apache.hbase:hbase-common:2.4.0   kafka_hbase_writer.py
