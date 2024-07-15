import time

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from utils import constants


def transform_data(value):
    # Implement your data transformation logic here
    transformed_value = value.upper()  # Example transformation
    return transformed_value


# Create Kafka Consumer
c = Consumer(
    {
        "bootstrap.servers": constants.BOOTSTRAP_SERVERS,
        "group.id": constants.GROUP_ID,
        "auto.offset.reset": constants.OFFSET_RESET,
    }
)

# Create Kafka Producer
p = Producer({"bootstrap.servers": constants.BOOTSTRAP_SERVERS})

is_sub = False
while True:
    try:
        if not is_sub:
            c.subscribe([constants.KAFKA_TOPIC])

        msg = c.poll(1.0)
        if msg is None:
            continue

        if msg.error():
            raise KafkaException(msg.error())

        print("Received message: {}: {}".format(msg.key(), msg.value()))
        is_sub = True

        # Transform the data
        transformed_value = transform_data(msg.value())

        # Publish the transformed data to another topic
        print(f"Producing message: key={msg.key()} value={transformed_value}")
        p.produce(
            constants.KAFKA_TOPIC_TRANSFORMED, key=msg.key(), value=transformed_value
        )
        p.flush()

    except KafkaException as e:
        if e.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
            time.sleep(5)  # Wait before retrying
        else:
            raise e


c.close()
p.flush()

