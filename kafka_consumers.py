from kafka import KafkaConsumer
import json
import datetime as dt
import logging

KAFKA_URL = 'localhost:9092'
TOPIC = 'weather_data'

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_URL,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

logging.info('waiting for messages...')

def consume_messages():
    for message in consumer:
        print(f'Received {dt.datetime.now()}: {message.value}')


if __name__ == '__main__':
    consume_messages()