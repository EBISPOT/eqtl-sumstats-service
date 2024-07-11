import time


def extract_data():
    print("Extracting data from eQTL FTP...")
    # Simulate data extraction
    time.sleep(5)
    data = "sample data"
    send_to_kafka(data)
    print("Data extraction complete.")


def send_to_kafka(data):
    from constants import BOOTSTRAP_SERVERS, KAFKA_TOPIC
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS, api_version=(0, 11, 5)
    )
    producer.send(KAFKA_TOPIC, value=data.encode("utf-8"))
    producer.flush()


if __name__ == "__main__":
    while True:
        extract_data()
        time.sleep(5)
