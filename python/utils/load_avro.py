'''
Author: billydharmawan
Author: Steve Mwangi
Link: https://medium.com/better-programming/avro-producer-with-python-and-confluent-kafka-library-4a1a2ed91a24
'''
from confluent_kafka import avro

'''
Takes avsc file and loads it, returning key and value
'''
def load_avro_schema(schema_file):
    key_schema_string = """
    {"type": "string"}
    """

    key_schema = avro.loads(key_schema_string)
    value_schema = avro.load(schema_file)

    return key_schema, value_schema