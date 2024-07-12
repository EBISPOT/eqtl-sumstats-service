from confluent_kafka import Consumer, Producer

from utils import constants

# Create Kafka Consumer
c = Consumer(
    {
        "bootstrap.servers": constants.BOOTSTRAP_SERVERS,
        "group.id": constants.GROUP_ID,
        "auto.offset.reset": constants.OFFSET_RESET,
    }
)

# Subscribe to topic
c.subscribe([constants.KAFKA_TOPIC, constants.KAFKA_TOPIC_TRANSFORMED])

# Create Kafka Producer
p = Producer({"bootstrap.servers": constants.BOOTSTRAP_SERVERS})

def transform_data(value):
    # Implement your data transformation logic here
    transformed_value = value.upper()  # Example transformation
    return transformed_value

try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(msg.error())
            break

        print("Received message: {}: {}".format(msg.key(), msg.value()))

        # Transform the data
        transformed_value = transform_data(msg.value())

        # Publish the transformed data to another topic
        p.produce(constants.KAFKA_TOPIC_TRANSFORMED, key=msg.key(), value=transformed_value)
        p.flush()

except KeyboardInterrupt:
    pass
finally:
    c.close()
    p.flush()
