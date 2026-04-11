# ⚡ SimpleKafka — Distributed Messaging System Built From Scratch

> A minimal Kafka-like system built to understand distributed systems at the protocol, storage, and coordination level.

---

## 🚀 Demo & Benchmarks

### 📊 Benchmark Results

| Messages | Time (s) | Throughput (msg/s) |
| -------- | -------- | ------------------ |
| 1,000    | 0.87     | 1149.43            |
| 5,000    | 4.21     | 1187.65            |

> Results depend on disk, JVM, and ZooKeeper load.

---

## 🧠 What This Project Demonstrates

* Designing distributed systems from scratch
* Building a custom binary protocol
* Implementing log-based storage (Kafka-style)
* Using ZooKeeper for coordination & leader election
* High-performance I/O with Java NIO

---

## ⚡ Features

### Core System

* Multi-broker architecture
* Topic + partition model
* Leader–Follower replication (basic)
* Controller election via ZooKeeper

### Storage Engine

* Append-only log
* Segmented storage (log + index)
* Offset-based reads
* FileChannel-based disk I/O

### Networking

* Custom binary wire protocol
* Non-blocking socket communication

### Client APIs

* Producer API
* Consumer API
* Metadata discovery

---

## 🏗️ Architecture

![Architecture](<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/a5df0e1e-4ce5-4371-856e-c6234bbf31d1" />
)

**Components:**

* Producer → sends messages
* Broker → stores + coordinates
* Consumer → fetches messages
* ZooKeeper → metadata + controller election

---

## 🧠 Design Decisions

### 1. Append-Only Log Storage

* Sequential disk writes → high throughput
* Avoids random I/O bottlenecks
* Enables replay + durability

### 2. Binary Wire Protocol

* Lower latency vs JSON/HTTP
* Compact payloads
* Efficient parsing

### 3. Partition-Based Scaling

* Horizontal scalability via partitions
* Parallelism across consumers

### 4. ZooKeeper for Coordination

* Broker discovery + controller election
* Ephemeral nodes for failure detection

### 5. Leader–Follower Replication

* Leader handles writes
* Followers replicate

### 6. Java NIO

* Non-blocking I/O
* Better performance vs traditional streams

### 7. Offset-Based Consumption

* Stateless consumers
* Replay capability

---

## 🔁 Data Flow

1. Producer sends message → Broker
2. Broker appends to partition log
3. Offset assigned
4. Replication to followers
5. Consumer fetches using offset

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
└── benchmark/
    └── Benchmark
```

---

## ⚙️ Tech Stack

* Java 21+
* Maven 3.9+
* Apache ZooKeeper
* Java NIO

---

## ⚡ Quick Start

### 1. Build

```bash
mvn clean package
```

---

### 2. Start ZooKeeper

```bash
bin/zookeeper-server-start.sh config/zoo.cfg
```

---

### 3. Start Broker

```bash
java -jar target/simple-kafka-1.0-SNAPSHOT-shaded.jar 0 localhost 9092 2181
```

---

### 4. Produce

```bash
java -cp target/simple-kafka-1.0-SNAPSHOT-shaded.jar com.simplekafka.client.SimpleKafkaProducer localhost 9092 test
```

---

### 5. Consume

```bash
java -cp target/simple-kafka-1.0-SNAPSHOT-shaded.jar com.simplekafka.client.SimpleKafkaConsumer localhost 9092 test 0
```

---

## 📊 Benchmark Script

```bash
./benchmark.sh localhost 9092 benchmark-topic 1000
```

---

## ⚠️ Limitations

* No consumer groups
* Basic replication
* No fault-tolerant leader recovery
* No batching / compression

---

## 🧠 Key Learnings

* Log-based storage systems
* Distributed coordination
* Binary protocols
* Partition scaling

---

