import time

from kafka import KafkaProducer
from utils import constants


def extract_data(index):
    print("Extracting data from eQTL FTP...")
    # Simulate data extraction
    time.sleep(5)
    data = "sample data"
    key = f"key_{index}"  # Generate a unique key for each message
    send_to_kafka(data, key)
    print("Data extraction complete.")


def send_to_kafka(data, key):
    producer = KafkaProducer(
        bootstrap_servers=constants.BOOTSTRAP_SERVERS,
        api_version=(0, 11, 5),
        key_serializer=str.encode,
        value_serializer=str.encode,
    )
    producer.send(constants.KAFKA_TOPIC, key=key, value=data)
    producer.flush()


if __name__ == "__main__":
    index = 1
    while index < 10:
        extract_data(index)
        index += 1
        time.sleep(1)
