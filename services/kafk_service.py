from kafka import KafkaProducer
import json
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

load_dotenv()

# Create Kafka producer with optimizations for high throughput
producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVER"), 
    sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM"),
    security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL"),
    sasl_plain_username=os.getenv("KAFKA_CLIENT_USERNAME"),
    sasl_plain_password=os.getenv("KAFKA_CLIENT_PASSWORD"),
    key_serializer=lambda k: k if isinstance(k, bytes) else k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=5,  # Batch messages for 5ms to optimize throughput
    compression_type='gzip',  # Compress data to save bandwidth
    batch_size=32 * 1024,  # 32 KB batch size for better performance
    max_in_flight_requests_per_connection=10,  # Allow 5 concurrent requests per connection
    acks='all'  # Wait for leader and ISR replication
)

# Thread pool for sending messages asynchronously
executor = ThreadPoolExecutor(max_workers=10)

def produce_message(topic_name, key, message):
    try:
        # Asynchronously send the message
        future = producer.send(topic_name, key=key, value=message)
        executor.submit(future.get)  # Handling the future asynchronously
    except Exception as e:
        print(f"Failed to send message: {e}")

# Optional: To gracefully close producer and executor on shutdown
def close_producer():
    producer.flush()  # Ensure all messages are sent
    producer.close()  # Close producer connection
    executor.shutdown(wait=True)  # Shutdown thread pool
