from confluent_kafka import Consumer

from constants import BOOTSTRAP_SERVERS, GROUP_ID, KAFKA_TOPIC, OFFSET_RESET

c = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": OFFSET_RESET,
    }
)

c.subscribe([KAFKA_TOPIC])


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
