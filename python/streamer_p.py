#!/usr/bin/env python3#
'''
Author: Steve G. Mwangi

Description: This code is for generating random telemetry data and streaming that data
to a PostgreSQL+Timescale DB, then visualizing the data on a grafana dashboard hosted at:
www.iotdapse.com
'''
import threading, sys, io, os, random, datetime, time, json, uuid, psycopg2, ast, concurrent.futures
from configparser import ConfigParser
from confluent_kafka import Producer, Consumer, KafkaError
from time import sleep
from datetime import datetime, timedelta

file = 'config.ini'
config = ConfigParser()
config.read(file)

kafka_hosts = config['account']['KAFKA_HOSTS']
kafka_settings = {'bootstrap.servers': 'kafka1, kafka2, kafka3','group.id': 'mygroup','client.id': 'client-1','enable.auto.commit': True,'session.timeout.ms': 6000,'default.topic.config': {'auto.offset.reset': 'smallest'}}
kafka_topic = config['account']['TOPIC']

p_producer = Producer({'bootstrap.servers': kafka_hosts})

# Seed values to generate streaming data
seed = {
        'Time':0,
        'X': -122.3583314,
        'Y' : 47.73388158,
        'Address': '115 NW 145TH ST',
        'City': 'SEATTLE',
        'State': 'WA',
        'Zip' : 98177,
        'PM_2_5': 8.77,
        'O3': 44.63, 
        'NO2': 36.45, 
        'SO2':64.23, 
        'CO':4.22,	
        'Temperature': 73.12, 
        'Humidity': 34.30, 
        'Energy': 0.143
    }

sql_schema = """INSERT INTO sensor_data (Time,X,Y,Address,City,State,Zip,PM_2_5,O3,NO2,SO2,CO,Temperature,Humidity,Energy)  VALUES (%(Time)s,%(X)s,%(Y)s,%(Address)s,%(City)s,%(State)s,%(Zip)s,%(PM_2_5)s,%(O3)s,%(NO2)s,%(SO2)s,%(CO)s,%(Temperature)s,%(Humidity)s,%(Energy)s);"""

# Confluent's function
def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: {0}: {1}"
              .format(msg.value(), err.str()))
    else:
        print("Message produced: {0}".format(msg.value()))


# little CPU bound: Function to randomly change seed vals
def mutator():
    global seed
    
    # 'PM_2_5'
    change_PM_2_5 = (seed['PM_2_5'] * random.random() / 100) * random.choice([-1,0,1])
    seed['PM_2_5'] += round(change_PM_2_5, 2)
    seed['PM_2_5'] = round(seed['PM_2_5'], 2)
    if seed['PM_2_5'] < 0:
        seed['PM_2_5'] = 0
    # 'O3'
    change_O3 = (seed['O3'] * random.random() / 100) * random.choice([-1,0,1])
    seed['O3'] += round(change_O3,2)
    seed['O3'] = round(seed['O3'],2)
    if seed['O3'] < 0:
        seed['O3'] = 0
    
    # 'NO2'
    change_NO2 = (seed['NO2'] * random.random() / 100) * random.choice([-1,0,1])
    seed['NO2'] += round(change_NO2, 2)
    seed['NO2'] = round(seed['NO2'], 2)
    if seed['NO2'] < 0:
        seed['NO2'] = 0

    # 'SO2'
    change_SO2 = (seed['SO2'] * random.random() / 100) * random.choice([-1,0,1])
    seed['SO2'] += round(change_SO2,2)
    seed['SO2'] = round(seed['SO2'],2)
    if seed['SO2'] < 0:
        seed['SO2'] = 0
    
    # 'CO'
    change_CO = (seed['CO'] * random.random() / 100) * random.choice([-1,0,1])
    seed['CO'] += round(change_CO, 2)
    seed['CO'] = round(seed['CO'], 2)
    if seed['CO'] < 0:
        seed['CO'] = 0

    # 'Temperature'
    change_Temperature = (seed['Temperature'] * random.random() / 100) * random.choice([-1,0,1])
    seed['Temperature'] += round(change_Temperature, 2)
    seed['Temperature'] = round(seed['Temperature'], 2)
    if seed['Temperature'] < 0:
        seed['Temperature'] = 0
    
    # 'Humidity'
    change_Humidity = (seed['Humidity'] * random.random() / 100) * random.choice([-1,0,1])
    seed['Humidity'] += round(change_Humidity, 2)
    seed['Humidity'] = round(seed['Humidity'], 2)
    if seed['Humidity'] < 0:
        seed['Humidity'] = 0
    
    # 'Energy'
    change_Energy = (seed['Energy'] * random.random() / 100) * random.choice([-1,0,1])
    seed['Energy'] += round(change_Energy, 2)
    seed['Energy'] = round(seed['Energy'], 2)
    if seed['Energy'] < 0:
        seed['Energy'] = 0
    return time.localtime()

# Producer function
def start_producing():
    global seed
    global p_producer
    global kafka_topic
    for i in range(10000):
        try:
            # print(i, "Producing seed: ", seed)
            p_producer.produce(kafka_topic, '{0}'.format(seed), callback=acked)
            with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(mutator)
                    print("Mutated data line {}".format(i))
            # p_producer.poll(0.01)
        except KeyboardInterrupt:
            pass
    p_producer.flush()
    

# Function to create DB Table for PostgreSQL
def create_table(conn):
    #create sensor data hypertable
    query_create_sensordata_table = """CREATE TABLE IF NOT EXISTS sensor_data (
        Time TIMESTAMP,
        X NUMERIC,
        Y NUMERIC,
        Address TEXT,
        City TEXT,
        State TEXT,
        Zip  INTEGER,
        PM_2_5   NUMERIC,
        O3 INTEGER,
        NO2 NUMERIC,
        SO2  NUMERIC,
        CO NUMERIC,
        Temperature NUMERIC,
        Humidity NUMERIC,
        Energy NUMERIC
        );"""
    # Create Hyper Table
    query_create_sensordata_hypertable = "SELECT create_hypertable('sensor_data', 'time');"
    cur = conn.cursor()
    cur.execute(query_create_sensordata_table)   
    cur.execute(query_create_sensordata_hypertable)

    #commit changes to the database to make changes persistent
    conn.commit()
    cur.close()
    
# ALTER TABLE sensor_data
#   RENAME TO sensor_data_1;


# Start the work
if __name__=="__main__":
    # create_table(conn)
    start_time = time.perf_counter()
    start_producing()
    end_time = time.perf_counter()
    
    print(f"Finished producing in: {round((end_time-start_time))} seconds")
    

# SELECT TOP 5
# FROM sensor_data;