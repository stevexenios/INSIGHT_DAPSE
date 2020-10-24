import json
import uuid
import avro
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro import AvroProducer
from configparser import ConfigParser
from confluent_kafka import Producer, Consumer, KafkaError
# Running telemetry data returning functions from the 3 files 
from utils.house_1 import generate_house_1
from utils.house_4 import generate_house_2_and_4
from utils.house_5 import generate_house_3_and_5

h1_dict = {
   "type": "record",
   "namespace": "schema",
   "name": "h1",
   "doc": "control1",
   "fields": [
      {"name": "Tarriff","doc": "Values could be high, low or normal","type": ["null", "string"]},
      {"name": "Energy","doc": "Values are in Kilowatts consumed per hour(kWh/h)","type": ["null", "string", "int", "double"]},
      {"name": "Humidity","doc": "Measured as a percentage (%)","type": ["null", "string", "int", "double"]},
      {"name": "Temperature","doc": "Units are in degrees Farenheit (F)","type": ["null", "string", "int", "double"]},
      {"name": "CO","doc": "Units are in parts per million (ppm)","type": ["null", "string", "int", "double"]},
      {"name": "SO2","doc": "Units are in parts per billion (ppb)","type": ["null", "string", "int", "double"]},
      {"name": "NO2","doc": "Units are in parts per billion (ppb)","type": ["null", "string", "int", "double"]},
      {"name": "O3","doc": "Units are in parts per billion (ppb)","type": ["null", "string", "int", "double"]},
      {"name": "PM_2_5","doc": "Units are in micrograms per cubic meter (ug/m3)","type": ["null", "string", "int", "double"]},
      {"name": "Zip","type": ["null", "string", "int"]},
      {"name": "State","type": ["null", "string"]},
      {"name": "City","type": ["null", "string"]},
      {"name": "Address","type": ["null", "string"]},
      {"name": "Y","type": ["null", "string", "double"]},
      {"name": "X", "type": ["null", "string", "double"]}
   ]
}

h2_dict = {
   "type": "record",
   "namespace": "schema",
   "name": "h2",
   "doc": "control2",
   "fields": [
      {
         "name": "Tarriff",
         "doc": "Values could be high, low or normal",
         "type": ["null", "string"]
      },
      {
         "name": "Energy",
         "doc": "Values are in Kilowatts consumed per hour(kWh/h)",
         "type": ["null", "string", "int", "double"]
      },
      {
         "name": "Humidity",
         "doc": "Measured as a percentage (%)",
         "type": ["null", "string", "int", "double"]
         
      },
      {
         "name": "Temperature",
         "doc": "Units are in degrees Farenheit (F)",
         "type": ["null", "string", "int", "double"]
         
      },
      {
         "name": "CO",
         "doc": "Units are in parts per million (ppm)",
         "type": ["null", "string", "int", "double"]
         
      },
      {
         "name": "SO2",
         "doc": "Units are in parts per billion (ppb)",
         "type": ["null", "string", "int", "double"]
         
      },
      {
         "name": "NO2",
         "doc": "Units are in parts per billion (ppb)",
         "type": ["null", "string", "int", "double"]
         
      },
      {
         "name": "O3",
         "doc": "Units are in parts per billion (ppb)",
         "type": ["null", "string", "int", "double"]
         
      },
      {
         "name": "PM_2_5",
         "doc": "Units are in micrograms per cubic meter (ug/m3)",
         "type": ["null", "string", "int", "double"]
         
      },
      {
         "name": "Zip",
         "type": ["null", "string", "int"]
         
      },
      {
         "name": "State",
         "type": ["null", "string"]
         
      },
      {
         "name": "City",
         "type": ["null", "string"]
         
      },
      {
         "name": "Address",
         "type": ["null", "string"]
         
      },
      {
         "name": "Y",
         "type": ["null", "string", "double"]
         
      },
      {
         "name": "X",
         "type": ["null", "string", "double"]
         
      }
   ]
}

h3_dict = {
   "type": "record",
   "namespace": "schema",
   "name": "h3",
   "doc": "control3",
   "fields": [
      {
         "name": "Tarriff",
         "doc": "Values could be high, low or normal",
         "type": ["null", "string"]
         
      },
      {
         "name": "Energy",
         "doc": "Values are in Kilowatts consumed per hour(kWh/h)",
         "type": ["null", "string", "int", "double"]
         
      },
      {
         "name": "Humidity",
         "doc": "Measured as a percentage (%)",
         "type": ["null", "string", "int", "double"]
         
      },
      {
         "name": "Temperature",
         "doc": "Units are in degrees Celsius (C)",
         "type": ["null", "string", "int", "double"]
         
      },
      {
         "name": "CO",
         "doc": "Units are in parts per million (ppm)",
         "type": ["null", "string", "int", "double"]
         
      },
      {
         "name": "SO2",
         "doc": "Units are in parts per billion (ppb)",
         "type": ["null", "string", "int", "double"]
         
      },
      {
         "name": "NO2",
         "doc": "Units are in parts per billion (ppb)",
         "type": ["null", "string", "int", "double"]
         
      },
      {
         "name": "O3",
         "doc": "Units are in parts per billion (ppb)",
         "type": ["null", "string", "int", "double"]
         
      },
      {
         "name": "PM_2_5",
         "doc": "Units are in micrograms per cubic meter (ug/m3)",
         "type": ["null", "string", "int", "double"]
         
      },
      {
         "name": "Zip",
         "type": ["null", "string", "int"]
         
      },
      {
         "name": "State",
         "type": ["null", "string"]
         
      },
      {
         "name": "City",
         "type": ["null", "string"]
         
      },
      {
         "name": "Address",
         "type": ["null", "string"]
         
      },
      {
         "name": "Y",
         "type": ["null", "string", "double"]
         
      },
      {
         "name": "X",
         "type": ["null", "string", "double"]
         
      }
   ]
}

h4_dict = {
   "type": "record",
   "namespace": "schema",
   "name": "h4",
   "doc": "test1 [1:x, 2:y, 3:temperature(f), 4:CO (ppm), 5:energy, 6:tariff",
   "fields": [
      {
         "name": "Tarriff",
         "doc": "Values could be high, low or normal",
         "type": ["null", "string"]
         
      },
      {
         "name": "Energy",
         "doc": "Values are in Kilowatts consumed per hour(kWh/h)",
         "type": ["null", "string", "int", "double"]
         
      },
      {
         "name": "Temperature",
         "doc": "Units are in degrees Farenheit (F)",
         "type": ["null", "string", "int", "double"]
         
      },
      {
         "name": "CO",
         "doc": "Units are in parts per million (ppm)",
         "type": ["null", "string", "int", "double"]
         
      },
      {
         "name": "Y",
         "type": ["null", "string", "double"]
         
      },
      {
         "name": "X",
         "type": ["null", "string", "double"]
         
      }
   ]
}

h5_dict = {
   "type": "record",
   "namespace": "schema",
   "name": "h5",
   "doc": "test2 [1:address, 2:temperature(c), 3:co (ppm), 6:energy]",
   "fields": [
      {
         "name": "Energy",
         "doc": "Values are in Kilowatts consumed per hour(kWh/h)",
         "type": ["null", "string", "int", "double"]
         
      },
      {
         "name": "CO",
         "doc": "Units are in parts per million (ppm)",
         "type": ["null", "string", "int", "double"]
         
      },
      {
         "name": "Temperature",
         "doc": "Units are in degrees Celsius (C)",
         "type": ["null", "string", "int", "double"]
         
      },
      {
         "name": "Address",
         "doc": "Includes Street, City, State and Zip",
         "type": ["null", "string"]
         
      }
   ]
}

h1_schema = avro.schema.make_avsc_object(h1_dict, avro.schema.Names())
h2_schema = avro.schema.make_avsc_object(h2_dict, avro.schema.Names())
h3_schema = avro.schema.make_avsc_object(h3_dict, avro.schema.Names())
h4_schema = avro.schema.make_avsc_object(h4_dict, avro.schema.Names())
h5_schema = avro.schema.make_avsc_object(h5_dict, avro.schema.Names())

h1_serializer = AvroJsonSerializer(h1_schema)
h2_serializer = AvroJsonSerializer(h2_schema)
h3_serializer = AvroJsonSerializer(h3_schema)
h4_serializer = AvroJsonSerializer(h4_schema)
h5_serializer = AvroJsonSerializer(h5_schema)

# Get configuration file
file = './secret/config.ini'
config = ConfigParser()
config.read(file)

# Get configuration values
kafka_hosts = config['account']['KAFKA_HOSTS']
kafka_settings = {'bootstrap.servers': 'kafka1, kafka2, kafka3','group.id': 'mygroup','client.id': 'client-1','enable.auto.commit': True,'session.timeout.ms': 6000,'default.topic.config': {'auto.offset.reset': 'smallest'}}
kafka_topic = config['account']['TOPIC']


# Confluent's function for error delivering message
def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: {0}: {1}"
              .format(msg.value(), err.str()))
    else:
        print("Message produced: {0}".format(msg.value()))

# produce records
def send_record():

    # Configuring the producer
    producer_config = {
        "bootstrap.servers": kafka_hosts,
        "schema.registry.url": 'http://localhost:8081'
    }

    # Generating UUID for key
    key = str(uuid.uuid4())


    while True:
      # Functions returning dictionary of telemetry data
      # One key for house 1
      h1 = generate_house_1()
      # Two keys for house 2 and 4
      d2_4 = generate_house_2_and_4()
      # Two keys for house 3 and 5 
      d3_5 = generate_house_3_and_5()

      j1 = h1_serializer.to_json(h1)
      j2 = h2_serializer.to_json(d2_4['h2'])
      j3 = h3_serializer.to_json(d3_5['h3'])
      j4 = h4_serializer.to_json(d2_4['h4'])
      j5 = h5_serializer.to_json(d3_5['h5'])
        

      # Get json objects from dictionary of telemetry data
      vals = [j1, j2, j3, j4, j5]
      for i in range(5):
         p_producer = Producer({'bootstrap.servers': kafka_hosts})
         value = vals[i]
         try:
               p_producer.produce(kafka_topic, '{0}'.format(value), callback=acked)
         except Exception as e:
               print("Exception while producing record value - {} to topic - {}: {}".format(value, kafka_topic, e))
         else:
            print("Successfully producing record value - {} to topic - {}".format(value, kafka_topic))
            p_producer.flush(1)


if __name__ == "__main__":
    send_record()