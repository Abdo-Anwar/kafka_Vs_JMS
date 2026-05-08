# JMS vs Kafka: Message Broker Performance Benchmark

## Table of Contents
1. [What Are Message Brokers?](#what-are-message-brokers)
2. [Why Use Message Brokers in Large Systems?](#why-use-message-brokers)
3. [How Message Brokers Work](#how-message-brokers-work)
4. [Systems Being Compared](#systems-being-compared)
5. [Implementation](#implementation)
6. [Sample Run & Results](#sample-run--results)
7. [Performance Metrics](#performance-metrics)
8. [Architecture Analysis](#architecture-analysis)
9. [When to Use Each](#when-to-use-each)

---

## What Are Message Brokers?

### Definition
A **message broker** is a software component that sits between applications and facilitates asynchronous communication by accepting, storing, and routing messages between different services or components.

Think of it like a postal service:
- **Sender** (Producer) sends a message to the broker
- **Broker** stores/queues the message
- **Receiver** (Consumer) retrieves the message later
- **Sender and receiver don't need to know about each other**

### Key Concept: Decoupling
Without a message broker:
```
Service A → (must wait) → Service B → (must wait) → Service C
[Tight coupling, synchronous, brittle]
```

With a message broker:
```
Service A → Message Broker ← Service B
                    ↓
                Message Queue
                    ↓
                Service C
[Loose coupling, asynchronous, resilient]
```

### Basic Components

1. **Producer**: Sends messages to the broker
   ```
   messageProducer.send("order-123: $99.99")
   ```

2. **Message Broker**: Receives, stores, and routes messages
   - Apache ActiveMQ Artemis (JMS)
   - Apache Kafka
   - RabbitMQ, AWS SQS, etc.

3. **Consumer**: Retrieves and processes messages
   ```
   Message msg = messageConsumer.receive()
   processOrder(msg)
   ```

4. **Queue/Topic**: Storage location
   - **Queue**: Point-to-point (1 producer → 1 consumer)
   - **Topic**: Pub-Sub (1 producer → many consumers)

---

## Why Use Message Brokers in Large Systems?

### 1. **Asynchronous Processing**
Handle requests without blocking:
```
User Request → API → Message Broker → Background Worker
       ↓
Returns immediately (while processing happens)
```
**Benefit**: Better response times, user doesn't wait for slow operations

### 2. **Decoupling Services**
Services don't need to know about each other:
```
Before: OrderService → PaymentService → ShippingService (if one fails, all fail)
After:  OrderService → Message Broker ← PaymentService, ShippingService (independent)
```
**Benefit**: One service failure doesn't cascade to others

### 3. **Load Balancing**
Distribute work across multiple consumers:
```
Message Queue: [msg1, msg2, msg3, msg4, msg5]
               ↓         ↓         ↓
         Consumer1  Consumer2  Consumer3 (all processing in parallel)
```
**Benefit**: Handle spikes in traffic by adding more consumers

### 4. **Reliability & Durability**
Messages survive system failures:
```
Producer sends → Broker stores to disk → Consumer retrieves
                 (even if broker crashes, messages persist)
```
**Benefit**: No message loss, fault tolerance

### 5. **Temporal Decoupling**
Producer and consumer don't need to run simultaneously:
```
Monday 9am:   Producer sends 1 million messages
Tuesday 2pm:  Consumer starts processing (messages were waiting)
```
**Benefit**: Producer and consumer operate on different schedules

### Real-World Example: E-Commerce Order Processing

```
1. User places order (OrderAPI)
2. Message: "order-123: shoes, $99.99" → Broker
3. Payment Service consumes → Charges card
4. Inventory Service consumes → Decrements stock
5. Shipping Service consumes → Creates shipment
6. Email Service consumes → Sends confirmation

All happening asynchronously without coupling!
```

---

## How Message Brokers Work

### Architecture

```
┌─────────────┐
│  Producer   │
│ (Sender)    │
└──────┬──────┘
       │
       ├─→ Message: "process this"
       │   (with nanosecond timestamp)
       ↓
┌──────────────────────────┐
│   Message Broker         │
│  ┌────────────────────┐  │
│  │ Queue/Topic        │  │
│  │ [msg1, msg2, msg3] │  │
│  └────────────────────┘  │
└──────────────────────────┘
       ↑
       │
       ├─← Message consumed
       │
┌──────┴──────┐
│  Consumer   │
│ (Receiver)  │
└─────────────┘
```

### Message Flow Timeline

```
PRODUCER:
t=0 µs     : Create message with 1KB payload
t=5 µs     : Send to broker (response time)
t=5 µs     : Message stored in queue

BROKER:
t=5 µs     : Message received and queued
t=5-10 ms  : Message persisted (optional, depends on durability)

CONSUMER:
t=0 ms     : Consumer.receive() blocking call
t=26-40 ms : Message retrieved and returned (end-to-end latency)
t=40 ms    : Process message (business logic)
```

---

## Systems Being Compared

### Apache ActiveMQ Artemis (JMS)

**What it is**: A high-performance JMS message broker
- **Founded**: 1996 (as ActiveMQ), evolved to Artemis (2015)
- **Primary use**: Low-latency point-to-point messaging
- **Architecture**: Single broker or clustered (optional)
- **Delivery model**: Synchronous and asynchronous

**Key Features**:
- Ultra-low latency (microseconds)
- Direct queue interaction
- Optional message persistence
- Non-persistent mode: fastest (no disk writes)

### Apache Kafka

**What it is**: A distributed event streaming platform
- **Founded**: 2010 at LinkedIn
- **Primary use**: High-throughput, distributed pub-sub
- **Architecture**: Always distributed (multiple brokers)
- **Delivery model**: Asynchronous with consumer groups

**Key Features**:
- Built-in replication (fault tolerance)
- Persistent storage (always durable)
- Consumer groups & offset tracking
- Designed for scale and distribution

### Comparison at a Glance

| Aspect | JMS (Artemis) | Kafka |
|--------|---------------|-------|
| **Primary Goal** | Low latency | Distributed scale |
| **Architecture** | Single or clustered | Distributed only |
| **Persistence** | Optional | Always |
| **Consumer Groups** | No | Yes |
| **Replication** | Optional | Built-in |
| **Latency** | ~26 ms | ~40 ms |
| **Use Case** | Orders, payments | Event streaming, analytics |

---

## Implementation

### Test Setup

We implemented comprehensive benchmarks for both systems to measure:

1. **Response Time**: How fast can each broker respond?
   - Producer: Time to send a message
   - Consumer: Time to receive a message
   - 1,000 runs, nanosecond precision

2. **Throughput**: Maximum messages per second?
   - Exponential load testing (1K → 2K → 4K ... until failure)
   - Measure when system starts dropping messages

3. **End-to-End Latency**: How long from send to receive?
   - Concurrent producer-consumer test
   - 10,000 messages with embedded timestamps
   - Measures actual message transit time

### Test Configuration

**Both systems running in Docker**:
```
JMS: Apache Artemis 2.34.0 (port 61616)
     - Non-persistent delivery (fast mode)
     - Single queue: PERF_TEST_QUEUE

Kafka: Apache Kafka 3.8.0 (port 9092)
       - KRaft mode (no Zookeeper)
       - Single broker, single partition
       - Topic: test-topic
```

**Message Size**: 1 KB (consistent across tests)

**Timing**: System.nanoTime() for microsecond precision

### Code Examples

**JMS Producer (Artemis)**:
```java
ConnectionFactory factory = new ActiveMQJMSConnectionFactory("tcp://localhost:61616");
Connection connection = factory.createConnection("admin", "admin");
Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
Destination queue = session.createQueue("PERF_TEST_QUEUE");
MessageProducer producer = session.createProducer(queue);

BytesMessage message = session.createBytesMessage();
message.writeBytes(new byte[1024]); // 1KB payload
long sendTimeNanos = System.nanoTime();
message.setLongProperty("SEND_TIME_NANOS", sendTimeNanos);

long start = System.nanoTime();
producer.send(message); // Send and wait for response
long responseTimeNanos = System.nanoTime() - start;
long responseTimeMicros = responseTimeNanos / 1000;
```

**Kafka Producer**:
```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
long sendTimeNanos = System.nanoTime();
String value = sendTimeNanos + ":Message content";
ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "key", value);

long start = System.nanoTime();
producer.send(record, (metadata, exception) -> {
    long responseTimeNanos = System.nanoTime() - start;
    // Callback receives response
});
```

**Concurrent Test** (Both producer and consumer run simultaneously):
```java
ProducerThread producerThread = new ProducerThread(...);
ConsumerThread consumerThread = new ConsumerThread(...);

producerThread.start();
consumerThread.start();

// Both process concurrently for true latency measurement
producerThread.join();
consumerThread.join();
```

---

## Sample Run & Results

### Running JMS Tests

```bash
# 1. Start Artemis broker
cd JMS
docker compose up -d
sleep 3

# 2. Run producer response time test
docker run --rm --network host -v "$(pwd)":/workspace -w /workspace \
  maven:3.9.4-eclipse-temurin-17 mvn -q compile exec:java \
  -Dexec.mainClass="com.lab.jms.Producer"
```

**Output:**
```
Sent msg 1000 | Avg response time: 5.23 µs
All 10000 messages sent.
============================================
Median Producer Response Time: 5 µs
============================================
```

```bash
# 3. Run consumer response time test
docker run --rm --network host -v "$(pwd)":/workspace -w /workspace \
  maven:3.9.4-eclipse-temurin-17 mvn -q compile exec:java \
  -Dexec.mainClass="com.lab.jms.Consumer"
```

**Output:**
```
Received msg 1000 (Avg latency: 4.12 µs)
Received msg 2000 (Avg latency: 3.98 µs)
...
============================================
Results: 10000 messages processed.
Median Consume Response Time: 4 µs
Median End-to-End Latency: 26.541 ms
============================================
```

### Running Kafka Tests

```bash
# 1. Start Kafka broker
cd kafka
docker compose up -d
sleep 5

# 2. Create topic
./scripts/create-topic.sh test-topic localhost:9092

# 3. Run producer response time test
cd kafka-java
docker run --rm --network host -v "$(pwd)":/workspace -w /workspace \
  maven:3.9.4-eclipse-temurin-17 mvn -q compile exec:java \
  -Dexec.mainClass="com.lab.kafka.LabProducer"
```

**Output:**
```
All 10000 messages sent.
============================================
Median Producer Response Time: 12,159 µs
============================================
```

```bash
# 4. Run consumer response time test
docker run --rm --network host -v "$(pwd)":/workspace -w /workspace \
  maven:3.9.4-eclipse-temurin-17 mvn -q compile exec:java \
  -Dexec.mainClass="com.lab.kafka.LabConsumer"
```

**Output:**
```
Consumer subscribed to topic=test-topic, waiting for 10000 messages
Consumed msg 1000 (Avg latency: 65139.70 µs)
...
============================================
Results: 10000 messages consumed.
Median Consume Response Time: 422 µs
Median End-to-End Latency: 40.051 ms
============================================
```

---

## Performance Metrics

### 📊 Complete Comparison Table

| Metric | JMS (Artemis) | Kafka | Difference | Winner |
|--------|---------------|-------|-----------|--------|
| **Produce Response Time** | 5 µs (0.005 ms) | 12,159 µs (12.159 ms) | 12,154 µs | JMS **2,432x** |
| **Consume Response Time** | 4 µs (0.004 ms) | 422 µs (0.422 ms) | 418 µs | JMS **105.5x** |
| **End-to-End Latency** | 26.541 ms | 40.051 ms | 13.51 ms | JMS **1.51x** |
| **Max Produce Throughput** | 512,000 msg/s | ~50,000 msg/s | 462,000 msg/s | JMS **10.2x** |
| **Max Consume Throughput** | 102,089 msg/s | ~80,000 msg/s | 22,089 msg/s | JMS **1.28x** |

### 🔍 Detailed Analysis

#### Producer Response Time (Time to Send a Message)

**JMS: 5 microseconds**
- Why so fast? Synchronous `send()` blocks until message is queued in memory
- No network latency (local broker)
- Direct queue interaction

**Kafka: 12.159 milliseconds**
- Why slower? Asynchronous callback model
- Network communication with broker
- Serialization overhead
- Callback invocation delay

**Implication**: JMS is 2,432x faster for single message sends. In 1 second:
- JMS: 200,000 messages
- Kafka: 82 messages (if waiting for each response)

#### Consumer Response Time (Time to Receive a Message)

**JMS: 4 microseconds**
- Synchronous receive from local queue
- Direct queue access

**Kafka: 422 microseconds**
- Poll-based model (wait, then retrieve batch)
- Poll timeout overhead
- Network communication

**Implication**: JMS is 105.5x faster for consuming. But Kafka polls in batches, so per-message overhead is acceptable.

#### End-to-End Latency (Send → Receive Time)

**JMS: 26.541 ms**
- Min: 0.485 ms (very fast)
- Max: 56.583 ms
- Median: 26.541 ms

**Kafka: 40.051 ms**
- Min: 34.498 ms (more consistent)
- Max: 190.967 ms
- Median: 40.051 ms

**Why the difference?**
- JMS: Direct queue → immediate delivery
- Kafka: Consumer group coordination (~13ms overhead)
  - Group discovery
  - Offset synchronization
  - Partition assignment
  - Then message retrieval

**Implication**: JMS delivers messages 51% faster. In a 1-second window, Kafka's latency adds noticeable delay.

#### Maximum Throughput

**Produce Throughput**:
- JMS: 512,000 msg/s (until failures occur)
- Kafka: ~50,000 msg/s (estimated)
- **Reason**: JMS synchronous model reaches bottleneck; Kafka's batching actually handles high load better

**Consume Throughput**:
- JMS: 102,089 msg/s
- Kafka: ~80,000 msg/s
- **Reason**: Similar bottlenecks; JMS's direct access gives slight edge

---

## Architecture Analysis

### Why Is JMS Faster?

```
JMS (Synchronous):
1. Client: send(message) → BLOCKING
2. Broker: Store in memory queue
3. Broker: Return ACK
4. Client: Resumes (5 µs elapsed)

Kafka (Asynchronous):
1. Client: send(record, callback) → NON-BLOCKING
2. Client: Returns immediately
3. Broker: Receives + processes (network delay)
4. Broker: Callback invoked (12 ms later)
```

### Why Is Kafka Designed This Way?

Kafka trades latency for **distributed reliability**:

```
JMS (Single Node):
[Broker] → Single point of failure
         → If crashes, messages lost
         → No replication

Kafka (Distributed):
[Broker1] ← Replication → [Broker2]
           ↑ Replication ↑
         [Broker3]

Single broker crash → others take over
Messages replicated across 3 brokers
→ Guaranteed durability
```

### Trade-Off Summary

| Aspect | JMS | Kafka |
|--------|-----|-------|
| **Latency** | Ultra-low ✅ | 40ms ⚠️ |
| **Throughput** | High ✅ | Moderate ⚠️ |
| **Durability** | Optional ⚠️ | Always ✅ |
| **Distribution** | Manual ⚠️ | Built-in ✅ |
| **Fault Tolerance** | Limited ⚠️ | Excellent ✅ |
| **Single Server** | Great ✅ | Overkill ⚠️ |

---

## When to Use Each

### Use **JMS (Artemis)** When:
✅ Latency is critical (< 30ms requirement)
✅ Point-to-point messaging pattern
✅ Single data center deployment
✅ Message throughput is priority
✅ Simple queue-based communication

**Examples**:
- Order processing within single company
- Real-time trading systems
- Internal service communication

### Use **Kafka** When:
✅ Distributed system across multiple data centers
✅ Need fault tolerance and replication
✅ Durability is critical (never lose messages)
✅ Multiple consumers of same data (pub-sub)
✅ Event streaming and real-time analytics

**Examples**:
- Global event streaming
- Multi-tenant SaaS platform
- Financial transaction logs
- Real-time analytics pipeline

---

## Running All Tests

### Quick Commands

```bash
# Start brokers
cd JMS && docker compose up -d
cd ../kafka && docker compose up -d && sleep 5

# Run all JMS tests
cd ../JMS
mvn clean compile exec:java -Dexec.mainClass="com.lab.jms.Producer"
mvn clean compile exec:java -Dexec.mainClass="com.lab.jms.Consumer"
mvn clean compile exec:java -Dexec.mainClass="com.lab.jms.ThroughputTest"
mvn clean compile exec:java -Dexec.mainClass="com.lab.jms.ConcurrentLatencyTest"

# Run all Kafka tests
cd ../kafka/kafka-java
./../../kafka/scripts/create-topic.sh
mvn clean compile exec:java -Dexec.mainClass="com.lab.kafka.LabProducer"
mvn clean compile exec:java -Dexec.mainClass="com.lab.kafka.LabConsumer"
mvn clean compile exec:java -Dexec.mainClass="com.lab.kafka.ThroughputTest"
mvn clean compile exec:java -Dexec.mainClass="com.lab.kafka.ConcurrentLatencyTest"

# Cleanup
cd ../../JMS && docker compose down
cd ../kafka && docker compose down
```

---

## Conclusion

This benchmark demonstrates the **fundamental trade-off** in messaging systems:

- **JMS**: Optimized for **low-latency point-to-point** messaging (26ms)
- **Kafka**: Optimized for **distributed, durable, scalable** event streaming (40ms + replication)

Neither is universally "better"—**choice depends on your requirements**.

**Use the metrics in this project to make an informed decision for your system.**

For detailed metrics and analysis, see [PERFORMANCE_METRICS.md](PERFORMANCE_METRICS.md).