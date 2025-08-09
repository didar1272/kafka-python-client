from kafka import KafkaProducer
import requests
import json
import time

KAFKA_URL = 'localhost:9092'
TOPIC = 'weather_data'
WEATHER_URL = "https://api.open-meteo.com/v1/forecast"

producer = KafkaProducer(
    bootstrap_servers = KAFKA_URL,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_weather_data() -> dict | None:
    weather_data = requests.get(url=WEATHER_URL, params={
        "latitude": 23.777176,
        "longitude": 90.399452,
        "current": "temperature_2m"
    })
    weather_data = weather_data.json()
    print(weather_data)
    return weather_data

def produce_messages():
    data = fetch_weather_data()
    producer.send(
        topic=TOPIC,
        value=data
    )
    print("Sent data: ", data)
    time.sleep(0.2)
    producer.flush()

if __name__ == "__main__":
    produce_messages()


