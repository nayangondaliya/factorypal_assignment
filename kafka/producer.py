#!/usr/bin/env python3

import json
import time
import random
from kafka import KafkaProducer
import os


def main():
    # Kafka broker connection settings (adjust as needed)
    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
    print(f"BOOTSTRAP_SERVERS: {bootstrap_servers}")
    topic_name = "sensor-input"

    # List of possible sensor IDs
    sensors = ["sensor-123", "sensor-456", "sensor-789", "sensor-321"]

    # Create the Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print(f"Publishing sensor data to Kafka topic '{topic_name}'...")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            # Pick a random sensor ID
            sensor_id = random.choice(sensors)

            # Generate a random measurement value
            value = round(random.uniform(10.0, 100.0), 2)

            # Current timestamp in milliseconds
            timestamp = int(time.time() * 1000)

            # Construct the event as a Python dictionary
            event = {
                "sensorId": sensor_id,
                "value": value,
                "timestamp": timestamp
            }

            # Send the event to the specified Kafka topic
            producer.send(topic_name, event)
            producer.flush()  # Ensure the message is immediately pushed

            print(f"Sent event: {event}")

            # Sleep for 1 second (adjust to control message rate)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping sensor data publisher.")
    finally:
        producer.close()
        print("Kafka producer closed.")


if __name__ == "__main__":
    main()
