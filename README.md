### EG/2020/4168 -Rupasinghe A.A.H.S
### Kafka Order Processing System


1. **Producer** generates random orders every 2 seconds
2. **Consumer** processes orders and calculates running average
3. **Retry mechanism** handles temporary failures (up to 3 attempts)
4. **DLQ** stores permanently failed messages


#### 1. Install Python Dependencies
```bash
pip install confluent-kafka avro-python3 fastavro
```

#### 2. Start Kafka Services
```bash
# Start Zookeeper
bin/windows/zookeeper-server-start.bat config/zookeeper.properties

# Start Kafka (in new terminal)
bin/windows/kafka-server-start.bat config/server.properties
```

#### 3. Create Topics
```bash
kafka-topics.bat --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics.bat --create --topic orders-retry --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics.bat --create --topic orders-dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

#### 4. Run Application
```bash
# Start consumer
python consumer.py

# Start producer (in new terminal)
python producer.py
```


