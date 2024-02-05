import paho.mqtt.client as mqtt
import os
import time
# import pandas as pd
from datetime import date
from datetime import datetime as dt

import shutil
import json
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.util.date_utils import get_date_helper

CSV_DIR = "results"

def convert_to_influx(measurement, timestamp):

    m_dict = {
        "measurement": "air_quality",
        "tags": {
            "type": measurement["name"],
            "sensor": measurement["sensor"],
            "unit": measurement["unit"]
        },
        "fields": {
            "value": float(measurement["value"])
        },
        "time": timestamp
    }

    return m_dict


def process_data(data, topic):

    data = data.strip('b')
    data = data.strip('\'')
    data = json.loads(data)
    timestamp = time.time_ns()
    print(timestamp)
    # helper = get_date_helper()
    # timestamp = helper.to_nanoseconds(timestamp)  
    data = [convert_to_influx(x, timestamp) for x in data]
    for datum in data:
        WRITE_API.write(bucket = BUCKET, record = datum)

    # path = get_today_file()
    # if not os.path.exists(path):
    #     with open(path, 'w') as file:
    #         file.write('datetime,sensor,temp,current,voltage,power\n')
    # s = data.split(',')
    # keysvals = [x.split(':') for x in s]
    # results_final = [dt.now(), topic]
    # for kv in keysvals:
    #     results_final.append(kv[1])
    
    # s_final = ','.join(str(x) for x in results_final)
    # with open(path, 'a') as file:
    #     file.write(s_final)
    #     file.write('\n')
    # shutil.copy(path, '/data/solar/')

        
    

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("sensors/air_quality/+")
    # client.subscribe('sensors/air_quality/+')

def on_message(client, userdata, msg):
    # print(msg.topic+" "+str(msg.payload))
    process_data(str(msg.payload), msg.topic)


client = mqtt.Client()


username = os.getenv('MOSQUITTO_USERNAME').strip("'")
password = os.getenv("MOSQUITTO_PASSWORD").strip("'")

client.username_pw_set(username=username,
                       password=password
                       )

influx_username = os.getenv('INFLUXDB_USERNAME')
influx_password = os.getenv('INFLUXDB_PASSWORD')

print(influx_password)
INFLUX_CLIENT = InfluxDBClient(url="http://localhost:8086",
                               org="air_quality_org",
                               username=influx_username,
                               password=influx_password)
BUCKET = 'air_quality_bucket'

WRITE_API = INFLUX_CLIENT.write_api(write_options=SYNCHRONOUS)
# QU = INFLUX_CLIENT.query_api()

# p = Point("my_measurement").tag("location", "Prague").field("temperature", 25.3)



# print(INFLUX_CLIENT.get_list_database())

client.on_connect = on_connect
client.on_message = on_message

client.connect("localhost", 1883, 60)

client.loop_forever()

