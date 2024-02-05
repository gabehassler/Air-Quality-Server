import os
import pandas as pd
from influxdb_client import InfluxDBClient
from influxdb_client.client.util.date_utils import get_date_helper

influx_username = os.getenv('INFLUXDB_USERNAME')
influx_password = os.getenv('INFLUXDB_PASSWORD')

INFLUX_CLIENT = InfluxDBClient(url="http://localhost:8086",
                               org="air_quality_org",
                               username=influx_username,
                               password=influx_password)
BUCKET = 'air_quality_bucket'

query_api = INFLUX_CLIENT.query_api()

# p = Point("my_measurement").tag("location", "Prague").field("temperature", 25.3)
tables = query_api.query_data_frame('from(bucket:"' + BUCKET + '") |> '
                                    'range(start: -10d) |> '
                                    'pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")')

# for table in tables:
#     print(table)
#     for row in table.records:
#         print (row.values)




print(tables)