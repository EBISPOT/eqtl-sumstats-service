# import time

# def extract_data():
#     print("Extracting data from eQTL FTP...")
    
#     f = open("demofile3.txt", "w")
#     f.write("Woops! I have deleted the content!")
#     f.close()
    
#     # Simulate data extraction
#     time.sleep(5)
#     print("Data extraction complete.")

# if __name__ == "__main__":
#     while True:
#         extract_data()
#         time.sleep(60)  # Run every 60 seconds


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
        time.sleep(60)  # Run every 60 seconds
