# Kafka Setup, Overhead, and Java Integration Notes

## Purpose

This file documents the local Kafka setup used for the lab, the operational overhead of running it, and the way the Java producer and consumer integrate with the broker.

## Local Setup

The lab uses Apache Kafka 3.8.0 with Java 17 on Ubuntu. The simplest manual setup is:

```bash
sudo apt update
sudo apt install openjdk-17-jdk -y
java -version

cd ~/Downloads
wget https://archive.apache.org/dist/kafka/3.8.0/kafka_2.13-3.8.0.tgz
tar -xvzf kafka_2.13-3.8.0.tgz
cd kafka_2.13-3.8.0

KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
bin/kafka-server-start.sh config/kraft/server.properties
```

In a second terminal:

```bash
cd ~/Downloads/kafka_2.13-3.8.0
bin/kafka-topics.sh --create \
  --topic test-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

## Setup Overhead

### Runtime requirements

- Java 17 is required for the lab code and Kafka client execution.
- Kafka runs as a separate background service, so the machine must keep a broker process alive while producer and consumer programs are running.
- Local persistence is needed for broker metadata and topic data, especially if Kafka is stopped and restarted.

### Operational overhead

- Manual installs add steps for download, extraction, storage formatting, and process startup.
- Kafka requires port management, typically `9092` for client access.
- Topic lifecycle operations must be handled explicitly, such as creating topics and verifying broker status.
- Even in KRaft mode, Kafka is still a server-side system, so it adds CPU, memory, and disk usage compared with a lightweight in-memory test setup.

### Lab-specific overhead

- Maven downloads the `kafka-clients` dependency during the first build.
- The Java producer and consumer need property files on the classpath.
- The lab uses scripts to reduce repeated command entry, but the broker still has to be started and monitored separately.

## Java Integration Research

The integration in this lab is done with the standard Apache Kafka Java client library, `org.apache.kafka:kafka-clients:3.8.0`.

### Producer integration

`LabProducer` loads `producer.properties` from the classpath, applies string serializers if they are not already configured, and sends `ProducerRecord<String, String>` messages to `test-topic`. A callback prints the partition and offset after each successful send.

### Consumer integration

`LabConsumer` loads `consumer.properties`, applies string deserializers if needed, subscribes to `test-topic`, and polls in a loop until the configured runtime window ends. Each received record is printed with topic, partition, offset, key, and value.

### Why this integration approach works well

- It uses the native Kafka client API, so there is no extra framework layer between the application and the broker.
- Configuration is externalized in properties files, which makes the code easier to reuse and adjust.
- The producer/consumer pattern is explicit, which is useful for a lab because it shows the broker interaction directly.

### Common integration options

- Plain Kafka clients, as used here, for direct control and low dependency overhead.
- Spring for Apache Kafka, if the application later needs Spring Boot integration, listener containers, and declarative messaging.
- Kafka Connect, if the goal is to move data between Kafka and external systems without custom code.

## Integration Flow in This Lab

1. Start Kafka in KRaft mode.
2. Create the `test-topic` topic.
3. Start the consumer so it is ready to receive records.
4. Start the producer and send messages.
5. Confirm messages appear in the consumer output.

## Notes

- The lab is intentionally minimal: one broker, one topic, one producer, and one consumer.
- For a larger deployment, the main extra costs are broker scaling, topic replication, monitoring, and storage management.
