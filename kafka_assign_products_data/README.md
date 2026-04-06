# Real-Time Data Processing with Confluent Kafka, MySQL, and Avro <br>
## 🎯 Objective <br>
### Build a robust real-time data pipeline using Kafka producer/consumer, SQL Server hosted on AWS RDS, Avro serialization, and multi-partition topics. <br>
**Producer**: Fetches incremental data from SQL Server hosted on AWS RDS → Serializes to Avro → Publishes to Kafka topic <br>
**Consumers**: Deserialize Avro → Append to separate JSON files <br>
## 🛠️ Tech Stack <br>
| Tool | Purpose | <br>
|------|---------| <br>
| Python 3.7+ | Core language | <br>
| Confluent Kafka | Kafka client | <br>
| MySQL | Database | <br>
| Apache Avro | Serialization | <br>
| VS Code | IDE |
