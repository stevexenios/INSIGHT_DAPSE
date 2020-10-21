#!/usr/bin/env python
'''
Author Steve G. Mwangi
Project: Dapse

Avro producer with cached schema data
'''
import json
import uuid

from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro import AvroProducer
from configparser import ConfigParser
from utils.load_avro import load_avro_schema

# Running telemetry data returning functions from the 3 files 
from utils.house_1 import generate_house_1
from utils.house_4 import generate_house_2_and_4
from utils.house_5 import generate_house_3_and_5

# Get configuration file
file = './secret/config.ini'
config = ConfigParser()
config.read(file)

# Get configuration values
kafka_hosts = config['account']['KAFKA_HOSTS']
kafka_settings = {'bootstrap.servers': 'kafka1, kafka2, kafka3','group.id': 'mygroup','client.id': 'client-1','enable.auto.commit': True,'session.timeout.ms': 6000,'default.topic.config': {'auto.offset.reset': 'smallest'}}
kafka_topic = config['account']['TOPIC']

# produce records
def send_record():

    # Configuring the producer
    producer_config = {
        "bootstrap.servers": kafka_hosts,
        "schema.registry.url": 'http://localhost:8081'
    }

    # Generating UUID for key
    key = str(uuid.uuid4())

    # Get schema from stored in registry
    schema_registry = CachedSchemaRegistryClient({"url": "http://localhost:8081"})
    value_schema = schema_registry.get_latest_schema("orders-value")[1]
    key_schema= schema_registry.get_latest_schema("orders-key")[1]
    print("SR: ", schema_registry)
    print("VS: ", value_schema)
    print("KS: ", key_schema)
    
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