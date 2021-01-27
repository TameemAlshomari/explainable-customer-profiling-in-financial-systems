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
            reader = csv.reader(file)
            headers = next(reader)
            dict_reader = csv.DictReader(file, [x.lower() for x in headers])
            next(dict_reader)
            print(headers)
            while True:
                line = next(dict_reader)
                del line['']
                # line = file.readline().split(',')
                json_msg = json.loads(json.dumps(line))
                # print(json_msg)
                partitions = producer.partitions_for(topic)
                producer.send(topic, json.dumps(json_msg).encode(), key=bytes(partitions.pop()))
                yield
    else:
        print('bootstrap server not connected')

def sendData(data,key,topic):
    producer.send(topic, data, key)
    
if __name__ == '__main__':
    import sys

    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    kafka_client = KafkaAdminClient(bootstrap_servers='localhost:9092')

    topic_name = sys.argv[1]
    file_path = sys.argv[2]

    new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
    topic = create_topic(kafka_client, topics=[new_topic])
    if topic_name == 'transfers':
        value_struct = {"schema":
               {"type":"struct",
                "fields":[{"type":"int64","optional":False,"field":"id"},
                          {"type":"string","optional":True,"field":"TRANSFER_ID"},
                          {"type":"string","optional":True,"field":"PARTY_ID"},
                          {"type":"string","optional":True,"field":"CURRENCY"},
                          {"type":"string","optional":True,"field":"AMOUNT"},
                          {"type":"string","optional":True,"field":"VALUE_DATE"},
                          {"type":"string","optional":True,"field":"CREATED_ON"}],
                "optional":False,
                "name":"transfers"},
               "payload":{}}
        
        tr = open (sys.argv[2], 'r')
        fieldnames = ["id", "TRANSFER_ID", "PARTY_ID", "CURRENCY", "AMOUNT", "VALUE_DATE"]
        reader = csv.DictReader(tr,fieldnames)
        next(reader) 
        i = 0
        for row in reader:  
            # print(i)  
            value_struct["payload"] = json.loads(json.dumps(row))
            print(value_struct['payload'])
            sendData(value_struct["payload"],row['id'],topic_name)
            sleep(0.002)
            i+=1
        producer.close()

    else:
        publisher = publish_to_topic(file_path, producer, topic)
        print(topic)
        # for i in range(1000):
        # print(i)
        while True:
            next(publisher)
            sleep(0.002)
        # print('wake up and repeat')

        producer.close()
