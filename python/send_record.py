
#!/usr/bin/env python

import json
import uuid

from confluent_kafka.avro import AvroProducer
from configparser import ConfigParser
from utils.load_avro import load_avro_schema

# Running telemetry data returning functions from the 3 files 
from utils.house_1 import generate_house_1
from utils.house_4 import generate_house_2_and_4
from utils.house_5 import generate_house_3_and_5

file = './secret/config.ini'
config = ConfigParser()
config.read(file)

kafka_hosts = config['account']['KAFKA_HOSTS']
kafka_settings = {'bootstrap.servers': 'kafka1, kafka2, kafka3','group.id': 'mygroup','client.id': 'client-1','enable.auto.commit': True,'session.timeout.ms': 6000,'default.topic.config': {'auto.offset.reset': 'smallest'}}
kafka_topic = config['account']['TOPIC']


def send_record():
    producer_config = {
        "bootstrap.servers": kafka_hosts,
        "schema.registry.url": 'http://localhost:8081'
    }

    key = str(uuid.uuid4())
    
    while True:
        # Functions returning dictionary of telemetry data
        # One key for house 1
        h1 = generate_house_1()
        # Two keys for house 2 and 4
        d2_4 = generate_house_2_and_4()
        # Two keys for house 3 and 5 
        d3_5 = generate_house_3_and_5()

        # Get json objects from dictionary of telemetry data
        vals = [json.dumps(h1), json.dumps(d2_4['h2']), json.dumps(d3_5['h3']), json.dumps(d2_4['h4']), json.dumps(d3_5['h5'])]
        for i in range(5):
            # Get schema from stored schema's
            file = './schema/h' + str(i+1) + '.avsc'
            key_schema, value_schema = load_avro_schema(file)
            producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)
            value = vals[i]
            try:
                producer.produce(topic=kafka_topic, key=key, value=value)
            except Exception as e:
                print("Exception while producing record value - {} to topic - {}: {}".format(value, kafka_topic, e))
            else:
                print("Successfully producing record value - {} to topic - {}".format(value, kafka_topic))
            producer.flush()


if __name__ == "__main__":
    send_record()