from kafka import KafkaConsumer
import json
import subprocess
import os

# Temp local file path
local_path = '/tmp/kafka_buffer.txt'

# HDFS file path
hdfs_path = '/user/training/basestations_kafka_consumer/output.txt'

# Setup Kafka consumer
consumer = KafkaConsumer(
    'govtech',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print(" Kafka consumer running...")

for msg in consumer:
    # Write message to local file
    with open(local_path, 'w') as f:
        f.write(json.dumps(msg.value) + '\n')
    
    # Append local file to HDFS using CLI
    try:
        subprocess.run(['hdfs', 'dfs', '-appendToFile', local_path, hdfs_path], check=True)
        print("Appended to HDFS:", msg.value)
    except subprocess.CalledProcessError as e:
        print(" Failed to append to HDFS:", e)

    # Optional: clean up temp file
    os.remove(local_path)
