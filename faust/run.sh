#!/usr/bin/env bash

# Please make sure that Apache Kafka server is running before you run this script

# install virtual environment package
python -m pip install virtualenv

# install virtual environment
python -m virtualenv venv

# make virtual environment executable
chmod +x venv/bin/activate

# enable virtual environment
./venv/bin/activate

# install requirements in virtual env
pip install -r requirements.txt

# run processing pipelines
echo "Running invoice stream processing pipeline"
faust -A invoice_history_processing worker -l info

# start kafka-producer for invoices
echo "Running kafka producer for invoices"
python kafka_producer.py invoices invoice_history.csv &


echo "Running transfer stream processing pipeline"
faust -A transfer_history_processing worker -l info

# start kafka-producer for transfers
echo "Running kafka producer for transfers"
python kafka_producer.py transfers transfer_history.csv &