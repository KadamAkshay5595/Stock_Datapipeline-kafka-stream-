from kafka import KafkaConsumer
from time import sleep
from json import dumps,load
import json
import pws as pw
import boto3
from json import dumps,loads
import time
 
 #create kafka consumer 
consumer = KafkaConsumer(
    'kafka-topic',
     bootstrap_servers=['host_name/ip_address:9092'], #add your IP here
    value_deserializer=lambda x: loads(x.decode('utf-8')))

# Specify your AWS credentials
aws_access_key_id = pw.aws_access_key_id
aws_secret_access_key = pw.aws_secret_access_key

# Create a Boto3 S3 client
s3_client = boto3.client('s3',
                         aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key)

# Specify the S3 bucket and file name

bucket_name = "Your-S3-bucket-name"
folder_name = "output/"

# for count, i in enumerate(consumer):
#      print(i.value)
#      print(count)
#      object_key = f"stock_market_{count}.json"
#      s3_client.put_object(Body=json.dumps(i.value), Bucket=bucket_name, Key=f"{folder_name}{object_key}")
     
for i in consumer:
   print(i.value)
   object_key = f"stock_market_{i.value['Index']}.json" 
   s3_client.put_object(Body=json.dumps(i.value), Bucket=bucket_name, Key=f"{folder_name}{object_key}")
   time.sleep(1)
