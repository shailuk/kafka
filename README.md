Real-Time Data Processing with Confluent Kafka, MySQL, and Avro
🎯 Objective
Build a robust real-time data pipeline using Kafka producer/consumer, MySQL CDC, Avro serialization, and multi-partition topics.

Producer: Fetches incremental data from MySQL → Serializes to Avro → Publishes to Kafka topic
Consumers: Deserialize Avro → Append to separate JSON files

🛠️ Tech Stack
Tool	Purpose
Python 3.7+	Core language
Confluent Kafka	Kafka Python client
MySQL	Source database
Apache Avro	Schema-based serialization
VS Code	Development environment
