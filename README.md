# SimpleKafka — Distributed Messaging System with Web UI

> A Kafka-like distributed messaging system built from scratch with a browser-based interface for real-time interaction.

---

## 🚀 Overview

SimpleKafka is a minimal distributed messaging system inspired by Apache Kafka.

It combines:
- a **distributed backend system** (broker, protocol, storage, coordination)
- a **web-based UI** for real-time interaction

This project focuses on understanding **how Kafka works internally** while also making it usable.

---

## 🚀 Demo & Benchmarks

### 📊 Benchmark Results

| Messages | Time (s) | Throughput (msg/s) |
|----------|----------|--------------------|
| 1,000    | 0.87     | 1149.43            |
| 5,000    | 4.21     | 1187.65            |

> Results vary based on system performance and ZooKeeper load.

---

## 🔥 Key Features

### Core System
- Multi-broker architecture
- Topic + partition model
- Leader–Follower replication (basic)
- ZooKeeper-based coordination & controller election

### Storage Engine
- Append-only log (Kafka-style)
- Segmented storage (log + index)
- Offset-based reads

### Networking
- Custom binary wire protocol
- TCP-based communication
- Non-blocking I/O

### Client APIs
- Producer API
- Consumer API
- Metadata discovery

### 🌐 Web UI (Major Differentiator)
- Create topics from browser
- Produce messages interactively
- Fetch messages using offsets
- View real-time structured output

---

## 🧠 What This Project Demonstrates

- Distributed systems design from scratch
- Binary protocol implementation
- Log-based storage architecture
- Coordination using ZooKeeper
- High-performance I/O using Java NIO
- Backend + frontend system integration

---

## 🏗️ Architecture

![Architecture](<img width="1536" height="1024" alt="f3445312-10cf-42ef-bbfb-906aba935834" src="https://github.com/user-attachments/assets/29f35beb-60fc-4b2f-ae8b-1278f68ba588" />
)

**Components:**
- Producer → sends messages
- Broker → handles storage, replication, coordination
- Consumer → fetches messages
- ZooKeeper → metadata + controller election
- Web UI → user interaction layer

---

## 🧠 Design Decisions

### 1. Append-Only Log Storage
- Sequential disk writes → high throughput
- Avoids random I/O bottlenecks
- Enables replay + durability

### 2. Binary Wire Protocol
- Lower overhead vs JSON/HTTP
- Compact payloads
- Efficient parsing

### 3. Partition-Based Scaling
- Horizontal scalability via partitions
- Enables parallel consumers

### 4. ZooKeeper for Coordination
- Broker registration
- Controller election
- Metadata management

### 5. Leader–Follower Replication
- Leader handles writes
- Followers replicate for redundancy

### 6. Java NIO
- Non-blocking I/O
- Efficient disk + network handling

### 7. Offset-Based Consumption
- Stateless consumers
- Supports replay and flexible reads

### 8. Web UI Layer
- Demonstrates real usability
- Bridges system internals with user interaction

---

## 🔁 Data Flow

1. Producer sends message → Broker  
2. Broker appends to partition log  
3. Offset assigned  
4. Message replicated to followers  
5. Consumer fetches using offset  

---

## 🌐 Web UI Usage

### Create Topic
- Enter topic name
- Set partitions & replication

### Produce Message
- Select topic
- Enter message
- Send

### Fetch Messages
- Choose topic + partition
- Set offset
- Fetch messages

---

## 📊 Example Output

```json
{
  "success": true,
  "messages": [
    { "offset": 0, "message": "hello there" },
    { "offset": 1, "message": "hello there" }
  ]
}
````

---

## 📂 Project Structure

```
src/main/java/com/simplekafka/
├── broker/
│   ├── SimpleKafkaBroker
│   ├── PartitionManager
│   ├── Partition
│   ├── ClusterManager
│   ├── ZookeeperClient
│   ├── Protocol
│   └── BrokerInfo
│
├── client/
│   ├── SimpleKafkaClient
│   ├── SimpleKafkaProducer
│   ├── SimpleKafkaConsumer
│   └── TestClient
│
├── frontend/
│   └── SimpleKafkaWebServer
│
└── web/
    └── index.html
```

---

## ⚙️ Tech Stack

* Java 21+
* Maven 3.9+
* Apache ZooKeeper
* Java NIO
* Lightweight HTTP server (UI)

---

## ⚡ Quick Start

### 1. Build

```bash
mvn clean package
```

---

### 2. Start ZooKeeper

```bash
zkServer.cmd start
```

---

### 3. Start Broker

```bash
java -cp target/simple-kafka-1.0-SNAPSHOT.jar com.simplekafka.broker.SimpleKafkaBroker 0 localhost 9092 2181
```

---

### 4. Start Web UI

```bash
java -cp target/simple-kafka-1.0-SNAPSHOT.jar com.simplekafka.frontend.SimpleKafkaWebServer 8080
```

Open:

```
http://localhost:8080
```

---

### 5. CLI Producer

```bash
java -cp target/simple-kafka-1.0-SNAPSHOT.jar com.simplekafka.client.SimpleKafkaProducer localhost 9092 test
```

---

### 6. CLI Consumer

```bash
java -cp target/simple-kafka-1.0-SNAPSHOT.jar com.simplekafka.client.SimpleKafkaConsumer localhost 9092 test 0
```

---

## 📊 Benchmark Script

```bash
./benchmark.sh localhost 9092 benchmark-topic 1000
```

---

## ⚠️ Limitations

* No consumer groups
* Basic replication (no ISR guarantees)
* No fault-tolerant leader recovery
* No batching / compression
* No security layer

---

## 📈 Roadmap

* [ ] Consumer groups
* [ ] Strong replication (ISR)
* [ ] Partition rebalancing
* [ ] Performance optimization
* [ ] Metrics & monitoring

---

## ⭐ Final Note

This project proves the ability to design and build **distributed systems end-to-end** — from protocol to UI.
