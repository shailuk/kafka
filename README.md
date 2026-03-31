# Real-Time Data Processing with Confluent Kafka, MySQL, and Avro <br>
## 🎯 Objective <br>
### Build a robust real-time data pipeline using Kafka producer/consumer, SQL Server hosted on AWS RDS, Avro serialization, and multi-partition topics. <br>

**Producer**: Fetches incremental data from SQL Server hosted on AWS RDS → Serializes to Avro → Publishes to Kafka topic
**Consumers**: Deserialize Avro → Append to separate JSON files

## 🛠️ Tech Stack
Tool	Purpose
Python 3.7+	Core language
Confluent Kafka	Kafka Python client
MySQL	Source database
Apache Avro	Schema-based serialization
VS Code	Development environment
