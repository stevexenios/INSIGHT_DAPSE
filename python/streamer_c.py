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

c_consumer = Consumer(kafka_settings)
c_consumer.subscribe([kafka_topic])

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


# into db
def insert_into_db(dictionary_y):
    sql_schema = """INSERT INTO sensor_data (Time,X,Y,Address,City,State,Zip,PM_2_5,O3,NO2,SO2,CO,Temperature,Humidity,Energy)  VALUES (%(Time)s,%(X)s,%(Y)s,%(Address)s,%(City)s,%(State)s,%(Zip)s,%(PM_2_5)s,%(O3)s,%(NO2)s,%(SO2)s,%(CO)s,%(Temperature)s,%(Humidity)s,%(Energy)s);"""
    with psycopg2.connect(config['account']['CONNECTION']) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_schema, dictionary_y)
    return time.localtime()


# Consumption
def start_consuming():
    global kafka_topic
    global c_consumer
    
    for i in range(10000):
        try:    
            msg = c_consumer.poll(1)
            print(msg)
            if not msg.error():
                d1 = msg.value()
                val = d1.decode("UTF-8")
                d = ast.literal_eval(val)
                d['Time'] = str(datetime.now()+timedelta(seconds=i))
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(insert_into_db, d)
                    print("Consumed message {} and inserted into db".format(i))
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                print('End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
            else:
                print('Error occured: {0}'.format(msg.error().str()))
        except KeyboardInterrupt:
            pass
    c_consumer.close()
        
        

# Start the work
if __name__=="__main__":
    # create_table(conn)
    start_time = time.perf_counter()
    start_consuming()
    end_time = time.perf_counter()
    
    print("Finished consuming in: {} seconds".format(round((end_time-start_time))))
    

# SELECT TOP 5
# FROM sensor_data;