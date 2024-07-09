from confluent_kafka import Consumer, KafkaException

c = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['etl_topic'])

try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        print('Received message: {}: {}'.format(msg.key(), msg.value()))
except KeyboardInterrupt:
    pass
finally:
    c.close()
