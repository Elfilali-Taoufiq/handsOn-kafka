import json

from kafka import KafkaConsumer

# Create Kafka consumer
actor_topic_consumer = KafkaConsumer(
    'postgres.sales',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='sales-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

test_topic_consumer = KafkaConsumer(
    'test-topic',  # Kafka topic to consume messages from
    bootstrap_servers='localhost:9092',  # Kafka broker address
    auto_offset_reset='earliest',  # Start reading from the earliest messages
    group_id='my-consumer-group',  # Consumer group ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),  # Deserialize JSON messages
    key_deserializer=lambda x: x.decode('utf-8')  # Deserialize keys
)


# Function to consume messages
def consume_messages(consumer):
    for message in consumer:
        print(f"Received message: {message.value} with key: {message.key}")


if __name__ == "__main__":
    print("Starting Kafka consumer...")
    consume_messages(test_topic_consumer)
