# Kafka Lab - Producer/Consumer (Java 17, KRaft)

This repository contains the Kafka part of the lab:
- Kafka Docker setup (KRaft mode)
- Java producer implementation
- Java consumer implementation
- Config files and run scripts
- Report section draft (without performance results)

## Prerequisites

Make sure these are installed on your Ubuntu machine:

- Java 17
  - Check: `java -version`
- Maven 3.8+
  - Check: `mvn -version`
- Docker + Docker Compose plugin
  - Check: `docker --version`
  - Check: `docker compose version`
- Apache Kafka CLI available in your PATH (for `kafka-topics.sh`)  
  OR use your local Kafka installation directory directly

## Required Lab Compatibility

This project is configured to match your lab constraints:
- Kafka version: `3.8.0`
- Mode: `KRaft`
- Broker endpoint: `localhost:9092`
- Topic: `test-topic`
- Java version: `17`
- OS target: Ubuntu Linux

## Project Structure

```text
kafka/
├── docker-compose.yml
├── instructions.md
├── README.md
├── kafka-java/
│   ├── pom.xml
│   └── src/main/
│       ├── java/com/lab/kafka/
│       │   ├── LabProducer.java
│       │   └── LabConsumer.java
│       └── resources/
│           ├── producer.properties
│           └── consumer.properties
├── scripts/
│   ├── create-topic.sh
│   ├── run-producer.sh
│   └── run-consumer.sh
└── report/
    └── kafka-section.md
```

## Setup

From repository root:

```bash
cd ~/Downloads/kafka
chmod +x scripts/*.sh
```

## Run Steps (End-to-End)

### 1) Start Kafka (Docker, KRaft mode)

```bash
docker compose up -d
docker compose ps
```

Expected: `kafka-lab` container is running.

### 2) Create the topic

```bash
./scripts/create-topic.sh
```

Default topic is `test-topic`.

### 3) Start consumer (Terminal 1)

```bash
./scripts/run-consumer.sh
```

Consumer listens to `test-topic` and prints received records.

### 4) Run producer (Terminal 2)

```bash
./scripts/run-producer.sh
```

Producer sends messages to `test-topic`.  
You should see messages printed in both producer and consumer terminals.

## Configuration

### Producer config
File: `kafka-java/src/main/resources/producer.properties`

Main fields:
- `bootstrap.servers=localhost:9092`
- `topic=test-topic`
- `message.count=10`

### Consumer config
File: `kafka-java/src/main/resources/consumer.properties`

Main fields:
- `bootstrap.servers=localhost:9092`
- `group.id=lab-consumer-group`
- `topic=test-topic`
- `consumer.runtime.seconds=30`

## Build and Compile Manually

If you want to compile directly:

```bash
cd kafka-java
mvn compile
```

## Useful Commands

List topics:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Describe topic:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic test-topic
```

Stop Kafka container:

```bash
docker compose down
```

Stop Kafka container + remove volume data:

```bash
docker compose down -v
```

## Troubleshooting

- **`kafka-topics.sh: command not found`**
  - Kafka CLI is not in PATH. Use full path from your Kafka install, for example:
    - `~/Downloads/kafka_2.13-3.8.0/bin/kafka-topics.sh ...`

- **Cannot connect to `localhost:9092`**
  - Verify Kafka container is running:
    - `docker compose ps`
  - Check logs:
    - `docker compose logs kafka`

- **Maven dependency download fails**
  - Check internet/DNS and Maven Central access.
  - Retry:
    - `mvn -U clean compile`

- **Topic already exists**
  - This is fine; `create-topic.sh` uses `--if-not-exists`.

## Report File

A ready Kafka report section (excluding performance results) is available in:

- `report/kafka-section.md`

