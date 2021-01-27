#!/usr/bin/env bash

# Please make sure that Apache Kafka, and Apache Cassandra servers are running before you run this script

# install virtual environment
python3 -m virtualenv venv

# make virtual environment executable
chmod +x venv/bin/activate

# enable virtual environment
./venv/bin/activate

# install requirements in virtual env
pip install -r requirements.txt

# run processing pipelines
./venv/lib/python3.7/site-packages/pyspark/bin/spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions pipelines/invoices_pipeline.py
./venv/lib/python3.7/site-packages/pyspark/bin/spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions pipelines/static_data_pipeline.py
./venv/lib/python3.7/site-packages/pyspark/bin/spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions pipelines/transfers_pipeline.py

# start kafka-producer
python kafka_files/kafka-producer.py &

# run final pipeline with model training and streaming
./venv/lib/python3.7/site-packages/pyspark/bin/spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions customer_profiling.py
