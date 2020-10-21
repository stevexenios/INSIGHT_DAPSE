import avro 
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

# Running telemetry data returning functions from the 3 files 
from house_1 import generate_house_1
from house_4 import generate_house_2_and_4
from house_5 import generate_house_3_and_5

value_schema = avro.loads(value_schema_str)
key_schema = avro.loads(key_schema_str)
value = {"name": "Value"}
key = {"name": "Key"}


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


avroProducer = AvroProducer({
    'bootstrap.servers': 'mybroker,mybroker2',
    'on_delivery': delivery_report,
    'schema.registry.url': 'http://schema_registry_host:port'
    }, default_key_schema=key_schema, default_value_schema=value_schema)

avroProducer.produce(topic='my_topic', value=value, key=key)
avroProducer.flush()