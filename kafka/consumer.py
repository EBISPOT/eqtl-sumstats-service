from confluent_kafka import Consumer

from utils import constants

c = Consumer(
    {
        "bootstrap.servers": constants.BOOTSTRAP_SERVERS,
        "group.id": constants.GROUP_ID,
        "auto.offset.reset": constants.OFFSET_RESET,
    }
)

c.subscribe([constants.KAFKA_TOPIC])


try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(msg.error())
            break
        print("Received message: {}: {}".format(msg.key(), msg.value()))
except KeyboardInterrupt:
    pass
finally:
    c.close()
