import json
import sys

from kafka import KafkaConsumer


# Configuration for Kafka Consumer
def create_consumer(group_id):
    return KafkaConsumer(
        'user-logs',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        key_deserializer=lambda x: x.decode('utf-8')
    )


# Function to consume messages
def consume_messages(consumer_id):
    consumer = create_consumer(group_id=f"log-processing-group-{consumer_id}")
    print(f"Consumer {consumer_id} started...")

    for message in consumer:
        print(f"Consumer {consumer_id} received message: {message.value} with key: {message.key}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python kafka_consumer.py <consumer_id>")
        sys.exit(1)

    consumer_id = sys.argv[1]
    consume_messages(consumer_id)
