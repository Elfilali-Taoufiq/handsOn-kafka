from kafka import KafkaConsumer
import json

# Configuration for Kafka Consumer
kafka_consumer = KafkaConsumer(
    'test-topic',  # Kafka topic to consume messages from
    bootstrap_servers='localhost:9092',  # Kafka broker address
    auto_offset_reset='earliest',  # Start reading from the earliest messages
    group_id='my-consumer-group',  # Consumer group ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),  # Deserialize JSON messages
    key_deserializer=lambda x: x.decode('utf-8')  # Deserialize keys
)

# Function to consume messages
def consume_messages():
    for message in kafka_consumer:
        print(f"Received message: {message.value} with key: {message.key}")

if __name__ == "__main__":
    print("Starting Kafka consumer...")
    consume_messages()
