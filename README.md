# Real-Time Data Processing with Confluent Kafka, MySQL, and Avro <br>
<br>
## 🎯 Objective <br>
<br>
### Build a robust real-time data pipeline using Kafka producer/consumer, SQL Server hosted on AWS RDS, Avro serialization, and multi-partition topics. <br>
<br>
**Producer**: Fetches incremental data from SQL Server hosted on AWS RDS → Serializes to Avro → Publishes to Kafka topic <br>
**Consumers**: Deserialize Avro → Append to separate JSON files <br>
<br>
## 🛠️ Tech Stack <br>
| Tool | Purpose |
|------|---------|
| Python 3.7+ | Core language |
| Confluent Kafka | Kafka client |
| MySQL | Database |
| Apache Avro | Serialization |
| VS Code | IDE |
