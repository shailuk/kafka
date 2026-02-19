from confluent_kafka import Consumer
from dotenv import load_dotenv
import os
import socket
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
import json
import sys

# Load environment variables from .env file
load_dotenv()

# SQL Server connection parameters (from environment variables)
SERVER = os.getenv('DB_SERVER', 'localhost')
DATABASE = os.getenv('DB_NAME', 'Test_Shailesh')
USERNAME = os.getenv('DB_USERNAME', 'sa')
PASSWORD = os.getenv('DB_PASSWORD')

# Validate required credentials - SQL Server connection parameters
if not USERNAME or not PASSWORD or not SERVER or not DATABASE:
    raise ValueError("Required environment variables (DB_SERVER, DB_NAME, DB_USERNAME, DB_PASSWORD) are not set")

#Connecting to Kafka Cluster on the cloud 
KAFKA_CLUSTER_BOOTSTRAP_SERVER = os.getenv('KAFKA_CLUSTER_BOOTSTRAP_SERVERS')
KAFKA_CLUSTER_API_KEY = os.getenv('KAFKA_CLUSTER_API_KEY')
KAFKA_CLUSTER_API_SECRET = os.getenv('KAFKA_CLUSTER_API_SECRET')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_SCHEMA_REGISTRY_URL = os.getenv('KAFKA_SCHEMA_REGISTRY_URL')
KAFKA_SCHEMA_REGISTRY_KEY = os.getenv('KAFKA_SCHEMA_REGISTRY_KEY')
KAFKA_SCHEMA_REGISTRY_VALUE = os.getenv('KAFKA_SCHEMA_REGISTRY_VALUE')
KAFKA_SCHEMA_REGISTRY_API_KEY = os.getenv('KAFKA_SCHEMA_REGISTRY_API_KEY')
KAFKA_SCHEMA_REGISTRY_API_SECRET = os.getenv('KAFKA_SCHEMA_REGISTRY_API_SECRET')

#Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': KAFKA_SCHEMA_REGISTRY_URL,
  'basic.auth.user.info': '{}:{}'.format(KAFKA_SCHEMA_REGISTRY_API_KEY, KAFKA_SCHEMA_REGISTRY_API_SECRET)
})

# Fetch the latest Avro schema for the key
key_schema_subject = KAFKA_SCHEMA_REGISTRY_KEY
key_schema_str = schema_registry_client.get_latest_version(key_schema_subject).schema.schema_str
print("Key Schema from Registry---")
print(key_schema_str)
print("=====================")

# Fetch the latest Avro schema for the value
value_schema_subject = KAFKA_SCHEMA_REGISTRY_VALUE
value_schema_str = schema_registry_client.get_latest_version(value_schema_subject).schema.schema_str
print("Value Schema from Registry---")
print(value_schema_str)
print("=====================")

# Create Avro Serializers for key and value
key_deserializer = AvroDeserializer(schema_registry_client, key_schema_str)
value_deserializer = AvroDeserializer(schema_registry_client, value_schema_str)

#Validate required credentials - Kafka Cluster connection parameters
if not KAFKA_CLUSTER_API_KEY or not KAFKA_CLUSTER_API_SECRET or not KAFKA_CLUSTER_BOOTSTRAP_SERVER:
    raise ValueError("Required environment variables (KAFKA_CLUSTER_BOOTSTRAP_SERVERS, KAFKA_CLUSTER_API_KEY, KAFKA_CLUSTER_API_SECRET) are not set")

kafka_conf = {'bootstrap.servers': KAFKA_CLUSTER_BOOTSTRAP_SERVER,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': KAFKA_CLUSTER_API_KEY,
        'sasl.password': KAFKA_CLUSTER_API_SECRET,
        'client.id': socket.gethostname(),
        'group.id': 'my-group',
        'auto.offset.reset': 'earliest', 
        'enable.auto.commit': False}

#Creating Consumer Instance
consumer = Consumer(kafka_conf)
#Subscribing to the topic to consume messages from
consumer.subscribe([KAFKA_TOPIC]) 

#Continually read messages from Kafka
try:
    while True:
        msg = consumer.poll(1.0) # How many seconds to wait for message

        if msg is None:
            continue
        if msg.error():
            print('Consumer error: {}'.format(msg.error()))
            continue

        # 1. Prepare the context for the Key
        key_ctx = SerializationContext(msg.topic(), MessageField.KEY)
        key = key_deserializer(msg.key(), key_ctx)
        """
        print(key_ctx)
        print(type(key_ctx))
        print(key)
        print(type(key)) """

        # 2. Prepare the context for the Value
        val_ctx = SerializationContext(msg.topic(), MessageField.VALUE)
        value = value_deserializer(msg.value(), val_ctx)
        """
        print(val_ctx)
        print(type(val_ctx))
        print(value)
        print(type(value)) """

        print(f"Successfully processed record: {key} -> {value}")

        #Applying Business Logic here
        print("Business Logic - 1: Change the category column to uppercase.")
        if 'product_category' in value:
            value['product_category'] = value['product_category'].upper()
            print(f"Updated Value : {value}")

        print("Business Logic - 2: Discount of 10% if a particular product falls under a ELECTRONICS category.")
        if 'product_category' in value and value['product_category'] == 'ELECTRONICS':
            value['price'] = value['price'] * 0.9  # Apply 10% discount
            value['discount'] = '10%'
            print(f"Applied discount to product: {value}")

        #Dump the data into a JSON file after applying business logic
        file_name = sys.argv[1] + '.json' if len(sys.argv) > 1 else 'output.json'
        with open(file_name, 'a') as f:
            f.write(json.dumps(value) + ', \n')

        # 3. Manual Commit
        consumer.commit(asynchronous=False)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()