import csv
import json
import os
from threading import Thread
from time import sleep

import pandas
from kafka import KafkaProducer
from kafka.admin import NewTopic, KafkaAdminClient
from kafka.errors import TopicAlreadyExistsError


def create_topic(kafka_client: KafkaAdminClient, topics: list) -> str:
    try:
        kafka_client.create_topics(topics)
        print('Topic created')
        return topics[0].name
    except TopicAlreadyExistsError:
        print('topic already exists')
        return topics[0].name
    # else:
    #     print('could not create topic:', result.value)
    #     return None


def publish_to_topic(filename: str, producer: KafkaProducer, topic: str):
    if producer.bootstrap_connected():
        with open(filename, 'r') as file:
            # reader = csv.reader(file)
            # header = next(reader)
            while True:
                # line = next(reader)[:-1]
                line = file.readline()
                partitions = producer.partitions_for(topic)
                producer.send(topic, line.encode(), key=bytes(partitions.pop()))
                yield
    else:
        print('bootstrap server not connected')


# def listen_to_topic(consumer: KafkaConsumer, topic: NewTopic):
#     if not consumer.subscription():
#         consumer.subscribe(topics=[topic.name])
#         print('subscribed to ', consumer.subscription())
#     try:
#         # record = ConsumerRecord()
#         record = next(consumer)
#         print(record.value)
#         consumer.commit()
#     except StopIteration:
#         print('no more records')

# def publish(publisher, thread_name):
#     for i in range(100):
#         print(i)
#         next(publisher)
#         sleep(0.1)
#         print('Thread', thread_name, ': wake up and repeat')


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    kafka_client = KafkaAdminClient(bootstrap_servers='localhost:9092')

    csvs = ['pyspark_invoices.csv', 'pyspark_transfers.csv', 'pyspark_static_data_v1.csv']
    topic_name = 'customer-profiling'

    # thread = []

    new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
    topic = create_topic(kafka_client, topics=[new_topic])

    filenames = [os.path.sep.join((os.path.dirname(__file__), '..', 'preprocessed_data', csv)) for csv in csvs]
    # file_to_publish = os.path.sep.join()
    invoices = pandas.read_csv(filenames[0])
    transfers = pandas.read_csv(filenames[1])
    static_data = pandas.read_csv(filenames[2])
    invoices = invoices.drop(columns=['Unnamed: 0'])
    transfers = transfers.drop(columns=['Unnamed: 0'])
    static_data = static_data.drop(columns=['Unnamed: 0', 'index'])
    combined =invoices.merge(transfers, on='PARTY_ID', how='inner').merge(static_data, on='PARTY_ID', how='inner')
    print(combined.describe())
    combined.to_csv('combined.csv')

    publisher = publish_to_topic('combined.csv', producer, topic)
    print(topic)
    for i in range(100):
        print(i)
        next(publisher)
        sleep(1)
        print('wake up and repeat')
    # t = Thread(name=topic, target=publish, args=(publisher, topic))
    # thread.append(t)
    # t.start()

    # for t in thread:
    #     t.join()

    producer.close()
