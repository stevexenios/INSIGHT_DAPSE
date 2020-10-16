'''
Code sourced from: https://github.com/confluentinc/confluent-kafka-python
Author: messense@icloud.com
Title: Avro Producer example
User: Steve Mwangi
'''
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError


c = AvroConsumer({
    'bootstrap.servers': 'mybroker,mybroker2',
    'group.id': 'groupid',
    'schema.registry.url': 'http://127.0.0.1:8081'})

c.subscribe(['my_topic'])

while True:
    try:
        msg = c.poll(10)

    except SerializerError as e:
        print("Message deserialization failed for {}: {}".format(msg, e))
        break

    if msg is None:
        continue

    if msg.error():
        print("AvroConsumer error: {}".format(msg.error()))
        continue

    print(msg.value())

c.close()