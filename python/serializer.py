import json
import uuid
import avro.schema

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
         "type": ["null", "string"],
         "default": null
      },
      {
         "name": "Energy",
         "doc": "Values are in Kilowatts consumed per hour(kWh/h)",
         "type": ["null", "string", "int", "double"],
         "default": null
      },
      {
         "name": "Humidity",
         "doc": "Measured as a percentage (%)",
         "type": ["null", "string", "int", "double"],
         "default": null
      },
      {
         "name": "Temperature",
         "doc": "Units are in degrees Farenheit (F)",
         "type": ["null", "string", "int", "double"],
         "default": null
      },
      {
         "name": "CO",
         "doc": "Units are in parts per million (ppm)",
         "type": ["null", "string", "int", "double"],
         "default": null
      },
      {
         "name": "SO2",
         "doc": "Units are in parts per billion (ppb)",
         "type": ["null", "string", "int", "double"],
         "default": null
      },
      {
         "name": "NO2",
         "doc": "Units are in parts per billion (ppb)",
         "type": ["null", "string", "int", "double"],
         "default": null
      },
      {
         "name": "O3",
         "doc": "Units are in parts per billion (ppb)",
         "type": ["null", "string", "int", "double"],
         "default": null
      },
      {
         "name": "PM_2_5",
         "doc": "Units are in micrograms per cubic meter (ug/m3)",
         "type": ["null", "string", "int", "double"],
         "default": null
      },
      {
         "name": "Zip",
         "type": ["null", "string", "int"],
         "default": null
      },
      {
         "name": "State",
         "type": ["null", "string"],
         "default": null
      },
      {
         "name": "City",
         "type": ["null", "string"],
         "default": null
      },
      {
         "name": "Address",
         "type": ["null", "string"],
         "default": null
      },
      {
         "name": "Y",
         "type": ["null", "string", "double"],
         "default": null
      },
      {
         "name": "X",
         "type": ["null", "string", "double"],
         "default": null
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
         "type": ["null", "string"],
         "default": null
      },
      {
         "name": "Energy",
         "doc": "Values are in Kilowatts consumed per hour(kWh/h)",
         "type": ["null", "string", "int", "double"],
         "default": null
      },
      {
         "name": "Humidity",
         "doc": "Measured as a percentage (%)",
         "type": ["null", "string", "int", "double"],
         "default": null
      },
      {
         "name": "Temperature",
         "doc": "Units are in degrees Celsius (C)",
         "type": ["null", "string", "int", "double"],
         "default": null
      },
      {
         "name": "CO",
         "doc": "Units are in parts per million (ppm)",
         "type": ["null", "string", "int", "double"],
         "default": null
      },
      {
         "name": "SO2",
         "doc": "Units are in parts per billion (ppb)",
         "type": ["null", "string", "int", "double"],
         "default": null
      },
      {
         "name": "NO2",
         "doc": "Units are in parts per billion (ppb)",
         "type": ["null", "string", "int", "double"],
         "default": null
      },
      {
         "name": "O3",
         "doc": "Units are in parts per billion (ppb)",
         "type": ["null", "string", "int", "double"],
         "default": null
      },
      {
         "name": "PM_2_5",
         "doc": "Units are in micrograms per cubic meter (ug/m3)",
         "type": ["null", "string", "int", "double"],
         "default": null
      },
      {
         "name": "Zip",
         "type": ["null", "string", "int"],
         "default": null
      },
      {
         "name": "State",
         "type": ["null", "string"],
         "default": null
      },
      {
         "name": "City",
         "type": ["null", "string"],
         "default": null
      },
      {
         "name": "Address",
         "type": ["null", "string"],
         "default": null
      },
      {
         "name": "Y",
         "type": ["null", "string", "double"],
         "default": null
      },
      {
         "name": "X",
         "type": ["null", "string", "double"],
         "default": null
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
         "type": ["null", "string"],
         "default": null
      },
      {
         "name": "Energy",
         "doc": "Values are in Kilowatts consumed per hour(kWh/h)",
         "type": ["null", "string", "int", "double"],
         "default": null
      },
      {
         "name": "Temperature",
         "doc": "Units are in degrees Farenheit (F)",
         "type": ["null", "string", "int", "double"],
         "default": null
      },
      {
         "name": "CO",
         "doc": "Units are in parts per million (ppm)",
         "type": ["null", "string", "int", "double"],
         "default": null
      },
      {
         "name": "Y",
         "type": ["null", "string", "double"],
         "default": null
      },
      {
         "name": "X",
         "type": ["null", "string", "double"],
         "default": null
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
         "type": ["null", "string", "int", "double"],
         "default": null
      },
      {
         "name": "CO",
         "doc": "Units are in parts per million (ppm)",
         "type": ["null", "string", "int", "double"],
         "default": null
      },
      {
         "name": "Temperature",
         "doc": "Units are in degrees Celsius (C)",
         "type": ["null", "string", "int", "double"],
         "default": null
      },
      {
         "name": "Address",
         "doc": "Includes Street, City, State and Zip",
         "type": ["null", "string"],
         "default": null
      }
   ]
}

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
       