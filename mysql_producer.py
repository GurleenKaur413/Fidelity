from kafka import KafkaProducer
import pymysql
import json

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# MySQL connection using PyMySQL
conn = pymysql.connect(
    host='localhost',
    user='training',
    password='training',
    database='loudacre',
    cursorclass=pymysql.cursors.DictCursor
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM basestations")

for row in cursor:
    producer.send('govtech', value=row)
    print("Sent:", row)

producer.flush()
cursor.close()
conn.close()

