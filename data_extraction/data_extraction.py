import time
from kafka import KafkaProducer

def extract_data():
    print("Extracting data from eQTL FTP...")
    # Simulate data extraction
    time.sleep(5)
    data = "sample data"
    send_to_kafka(data)
    print("Data extraction complete.")

def send_to_kafka(data):
    producer = KafkaProducer(bootstrap_servers='kafka:9092')
    producer.send('etl_topic', value=data.encode('utf-8'))
    producer.flush()

if __name__ == "__main__":
    while True:
        extract_data()
        time.sleep(5)
