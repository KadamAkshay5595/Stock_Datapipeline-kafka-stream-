import json
from json import dumps
from kafka import KafkaProducer
import datetime
import time
import random
import pandas as pd

# Add your kafka parameters

topic = "kafka-topic"  # your kafka topic
producer = KafkaProducer(bootstrap_servers="host_name/ip_address:9092", value_serializer=lambda x:dumps(x).encode('utf-8'))

import pandas as pd

df = pd.read_csv("indexProcessed.csv")

for i in range(30):
    x = df.sample(1).to_dict(orient="records")[0]
    producer.send(topic, value=x)
    time.sleep(1)
    print(x)

