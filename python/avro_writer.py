import json
import uuid
import avro.schema

from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
# Running telemetry data returning functions from the 3 files 
from utils.house_1 import generate_house_1
from utils.house_4 import generate_house_2_and_4
from utils.house_5 import generate_house_3_and_5

# write 10000 lines for 5 avro files
def write_avro_files():
    # Read schemas
    s1 = avro.schema.parse(open("./schema/h1.avsc", "rb").read())
    s2 = avro.schema.parse(open("./schema/h2.avsc", "rb").read())
    s3 = avro.schema.parse(open("./schema/h3.avsc", "rb").read())
    s4 = avro.schema.parse(open("./schema/h4.avsc", "rb").read())
    s5 = avro.schema.parse(open("./schema/h5.avsc", "rb").read())

    # Write using schema avro files
    w1 = DataFileWriter(open("./avro/h1.avro", "wb"), DatumWriter(), s1)
    w2 = DataFileWriter(open("./avro/h2.avro", "wb"), DatumWriter(), s2)
    w3 = DataFileWriter(open("./avro/h3.avro", "wb"), DatumWriter(), s3)
    w4 = DataFileWriter(open("./avro/h4.avro", "wb"), DatumWriter(), s4)
    w5 = DataFileWriter(open("./avro/h5.avro", "wb"), DatumWriter(), s5)

    for i in range(10000):
        print(i)
        # Functions returning dictionary of telemetry data
        # One key for house 1
        w1.append(generate_house_1())
        # Two keys for house 2 and 4
        d2_4 = generate_house_2_and_4()
        w2.append(d2_4['h2'])
        w4.append(d2_4['h4'])
        # Two keys for house 3 and 5 
        d3_5 = generate_house_3_and_5()
        w3.append(d3_5['h3'])
        w5.append(d3_5['h5'])

    w1.close()
    w2.close()
    w3.close()
    w4.close()
    w5.close()

if __name__ == "__main__":
    write_avro_files()
       