from kafka import KafkaProducer
import time
import csv
import binascii
import json
import pandas as pd


#producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: v.encode('utf-8'))
producer = KafkaProducer(bootstrap_servers='localhost:9092',key_serializer=lambda v: v.encode('utf-8'), value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def preprocess_transfers():
    static_data = pd.read_csv('data/static_data.csv')
    transfers = pd.read_csv('data/transfer_history.csv')
    # print(static_data.info())
    transfers = transfers.drop(columns=['Unnamed: 0'])
    # static_data = static_data.drop(columns=['Unnamed: 0', 'index'])
    #Select wanted columns from static
    staticc = static_data[['PARTY_ID', 'CREATED_ON']]
    combined =transfers.merge(staticc, on='PARTY_ID', how='inner')
    print(combined.info())
    # print(staticc.info())
    combined.to_csv('data/combined.csv')


def sendData(data,key,topic):
    producer.send(topic, data, key)



# preprocess_transfers()

value_struct = {"schema":
               {"type":"struct",
                "fields":[{"type":"int64","optional":False,"field":"id"},
                          {"type":"string","optional":True,"field":"TRANSFER_ID"},
                          {"type":"string","optional":True,"field":"PARTY_ID"},
                          {"type":"string","optional":True,"field":"CURRENCY"},
                          {"type":"string","optional":True,"field":"AMOUNT"},
                          {"type":"string","optional":True,"field":"VALUE_DATE"}],
                "optional":False,
                "name":"transfers"},
               "payload":{}}


csvfile = open('data/transfer_history.csv', 'r')
fieldnames = ["id", "TRANSFER_ID", "PARTY_ID", "CURRENCY", "AMOUNT", "VALUE_DATE"]
# f_csv = csv.reader(csvfile) 
# headers = next(f_csv) 
reader = csv.DictReader(csvfile,fieldnames)
next(reader) 
i = 0
for row in reader:  
    # print(i)  
    value_struct["payload"] = json.loads(json.dumps(row))
    print(value_struct['payload'])
    sendData(value_struct["payload"],row['id'],'transfers')
    i+=1
producer.close()