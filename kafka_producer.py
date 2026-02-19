from operator import index
from time import time
import pyodbc
import json
import os
import socket
from dotenv import load_dotenv
from confluent_kafka import Producer
import time
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

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

#Validate required credentials - Kafka Cluster connection parameters
if not KAFKA_CLUSTER_API_KEY or not KAFKA_CLUSTER_API_SECRET or not KAFKA_CLUSTER_BOOTSTRAP_SERVER:
    raise ValueError("Required environment variables (KAFKA_CLUSTER_BOOTSTRAP_SERVERS, KAFKA_CLUSTER_API_KEY, KAFKA_CLUSTER_API_SECRET) are not set")

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': KAFKA_SCHEMA_REGISTRY_URL,
  'basic.auth.user.info': '{}:{}'.format(KAFKA_SCHEMA_REGISTRY_API_KEY, KAFKA_SCHEMA_REGISTRY_API_SECRET)
})

# Fetch the latest Avro schema for the key
key_schema_str = schema_registry_client.get_latest_version(KAFKA_SCHEMA_REGISTRY_KEY).schema.schema_str
print("Key Schema from Registry---")
print(key_schema_str)
print("=====================")

# Fetch the latest Avro schema for the value
value_schema_str = schema_registry_client.get_latest_version(KAFKA_SCHEMA_REGISTRY_VALUE).schema.schema_str
print("Value Schema from Registry---")
print(value_schema_str)
print("=====================")

# Create Avro Serializers for key and value
key_serializer = AvroSerializer(
    schema_registry_client, 
    key_schema_str,
    conf={'subject.name.strategy': lambda ctx, record: KAFKA_SCHEMA_REGISTRY_KEY}
)
value_serializer = AvroSerializer(
    schema_registry_client, 
    value_schema_str,
    conf={'subject.name.strategy': lambda ctx, record: KAFKA_SCHEMA_REGISTRY_VALUE}
)

kafka_conf = {'bootstrap.servers': KAFKA_CLUSTER_BOOTSTRAP_SERVER,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': KAFKA_CLUSTER_API_KEY,
        'sasl.password': KAFKA_CLUSTER_API_SECRET,
        'client.id': socket.gethostname()
        }

# Define the Producer
producer = Producer(kafka_conf)

#Declaring variables
batch_size=100
row_count = 0

#Generator function to fetch rows in batches
def fetch_rows(cursor, batch_size):
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        for row in rows:
            yield row

#Report Error if kafka producer fails to deliver message to the topic, else print success message with details of the produced message
def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
    print("=====================")

try:
    # Create connection string
    connection_string = f'Driver={{ODBC Driver 17 for SQL Server}};Server={SERVER};Database={DATABASE};UID={USERNAME};PWD={PASSWORD}'
    
    # Connect to SQL Server
    connection = pyodbc.connect(connection_string)
    print("✓ Successfully connected to SQL Server")
    
    # Create cursor
    cursor = connection.cursor()
    
    # Query the product_last_fetch table and get the last fetch time
    last_fetch_query = "SELECT last_fetch FROM product_last_fetch"
    cursor.execute(last_fetch_query)
    last_fetch_date = cursor.fetchone()
    last_fetch = last_fetch_date[0] if last_fetch_date else '1900-01-01 00:00:00'
    print(f"Last fetch time: {last_fetch}")
   
    #Query the product table and get the products updated since the last fetch time
    query = "SELECT * FROM product WHERE last_updated > ?"
    cursor.execute(query, (last_fetch,))
    
    for row in fetch_rows(cursor, batch_size):
        product_data = {
            'product_id': int(row[0]),
            'product_name': row[1],
            'product_category': row[2],
            'price': float(row[3]),
            'last_updated': str(row[4])
        }
        # Display results
        #print(json.dumps(product_data)) #, indent=2
        key = int(product_data['product_id'])
        value = product_data         
        print('key :', key)
        print('value :', value)
        row_count += 1

        # Serialize manually before producing
        msg_key = key_serializer(key)
        msg_value = value_serializer(value)

        # Produce to Kafka
        producer.produce(
            topic = KAFKA_TOPIC, 
            key = msg_key,
            value = msg_value,
            on_delivery = delivery_report)
        producer.flush()
        #time.sleep(2)
    
    print(f"\n✓ Retrieved {row_count} products from the database:\n")

    #updating the last_fetch date in the product_last_fetch table to current timestamp 
    update_query = "UPDATE product_last_fetch SET last_fetch = GETDATE()"
    cursor.execute(update_query)
    connection.commit()
    print("✓ Updated last_fetch time in product_last_fetch table")

    # Close connections
    cursor.close()
    connection.close()
    print("\n✓ Connection closed")

except Exception as e:
    print(f"✗ Error connecting to SQL Server: {e}")
    print("\nPlease ensure:")
    print("  - SQL Server is running")
    print("  - ODBC Driver 17 for SQL Server is installed")
    print("  - Update SERVER, USERNAME, PASSWORD in the script") 
    print("  - Check the connections related to Kafka Cluster and Schema Registry")


