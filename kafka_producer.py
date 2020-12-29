from time import sleep

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import TopicAlreadyExistsError

producer = None


def initialize_producer(force=False):
    global producer
    if producer is None:
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
    if force:
        producer = KafkaProducer(bootstrap_servers='localhost:9092')


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
            file.readline()
            while True:
                # producer.flush()
                # print(type(file))
                line = file.readline()
                # reader = csv.reader(file)
                # print(reader.next())
                partitions = producer.partitions_for(topic)
                producer.send(topic, line.encode(), key=bytes(partitions.pop()))
                yield
    else:
        print('bootstrap server not connected')


def listen_to_topic(consumer: KafkaConsumer, topic: NewTopic):
    if not consumer.subscription():
        consumer.subscribe(topics=[topic.name])
        print('subscribed to ', consumer.subscription())
    try:
        # record = ConsumerRecord()
        record = next(consumer)
        print(record.value)
        consumer.commit()
    except StopIteration:
        print('no more records')


if __name__ == '__main__':
    initialize_producer()
    kafka_client = KafkaAdminClient(bootstrap_servers='localhost:9092')

    new_topic = NewTopic(name='credit-card-fraud-topic', num_partitions=1, replication_factor=1)
    topic = create_topic(kafka_client, topics=[new_topic])
    file_to_publish = 'creditcard.csv'
    print(file_to_publish)
    consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                             auto_offset_reset='earliest',
                             consumer_timeout_ms=1000,
                             group_id=topic)
    publisher = publish_to_topic(file_to_publish, producer, topic)
    print(topic)
    for i in range(100):
        print(i)
        # print(type(producer))
        next(publisher)
        sleep(1)
        # print('wake up and listen')
        # listen_to_topic(consumer, new_topic)
        # sleep(1)
        print('wake up and repeat')
    consumer.close()
    producer.close()
