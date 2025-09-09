from kafka import KafkaConsumer
from hdfs import InsecureClient
import json


hdfs_client = InsecureClient('http://localhost:50070', user='training')

consumer = KafkaConsumer(
    'govtech',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

with hdfs_client.write('/user/training/basestations_kafka_consumer/results1.json', overwrite=True, encoding='utf-8') as writer:
    for msg in consumer:
        writer.write(json.dumps(msg.value) + '\n')
        print("Wrote to HDFS:", msg.value)
