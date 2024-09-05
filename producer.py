from kafka import KafkaProducer
import json
import time

# Configuration for Kafka Producer
kafka_producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serialize messages as JSON
    key_serializer=str.encode  # Serialize keys as UTF-8 encoded strings
)


# Function to produce messages
def produce_messages(topic, records_number):
    for i in range(records_number):
        # Create a message
        message = {
            'key': f'key-{i}',
            'value': f'value-{i}'
        }

        # Produce message to Kafka
        kafka_producer.send(topic, key=str(i), value=message)

        # Wait for message to be sent
        kafka_producer.flush()

        # Sleep for a bit before sending the next message
        time.sleep(1)


if __name__ == "__main__":
    topic = 'test-topic'  # Kafka topic to send messages to
    records_number=5
    produce_messages(topic,records_number)
