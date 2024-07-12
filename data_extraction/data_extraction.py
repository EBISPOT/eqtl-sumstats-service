import time


def extract_data():
    print("Extracting data from eQTL FTP...")
    # Simulate data extraction
    time.sleep(5)
    data = "sample data"
    send_to_kafka(data)
    print("Data extraction complete.")


def send_to_kafka(data):
    from kafka import KafkaProducer
    from utils import constants

    producer = KafkaProducer(
        bootstrap_servers=constants.BOOTSTRAP_SERVERS, api_version=(0, 11, 5),
    )
    producer.send(constants.KAFKA_TOPIC, value=data.encode("utf-8"),)
    producer.flush()


if __name__ == "__main__":
    index = 1
    while index > 0:
        index += 1
        extract_data()
        time.sleep(5)
