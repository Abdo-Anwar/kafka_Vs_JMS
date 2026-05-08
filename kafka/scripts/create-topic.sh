#!/usr/bin/env bash
set -euo pipefail

TOPIC_NAME="${1:-test-topic}"
BOOTSTRAP_SERVER="${2:-localhost:9092}"
PARTITIONS="${3:-1}"
REPLICATION_FACTOR="${4:-1}"

KAFKA_TOPICS_CMD=""

if command -v kafka-topics.sh >/dev/null 2>&1; then
  KAFKA_TOPICS_CMD="$(command -v kafka-topics.sh)"
elif [ -x "${HOME}/Downloads/kafka_2.13-3.8.0/bin/kafka-topics.sh" ]; then
  KAFKA_TOPICS_CMD="${HOME}/Downloads/kafka_2.13-3.8.0/bin/kafka-topics.sh"
fi

echo "Creating topic ${TOPIC_NAME} on ${BOOTSTRAP_SERVER}..."
if [ -n "${KAFKA_TOPICS_CMD}" ]; then
  "${KAFKA_TOPICS_CMD}" --create \
    --topic "${TOPIC_NAME}" \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION_FACTOR}" \
    --if-not-exists
elif command -v docker >/dev/null 2>&1; then
  CONTAINER_FOUND="false"
  while IFS= read -r name; do
    if [ "${name}" = "kafka-lab" ]; then
      CONTAINER_FOUND="true"
      break
    fi
  done < <(docker ps --format '{{.Names}}')

  if [ "${CONTAINER_FOUND}" = "true" ]; then
    docker exec kafka-lab /opt/kafka/bin/kafka-topics.sh --create \
      --topic "${TOPIC_NAME}" \
      --bootstrap-server "${BOOTSTRAP_SERVER}" \
      --partitions "${PARTITIONS}" \
      --replication-factor "${REPLICATION_FACTOR}" \
      --if-not-exists
  else
    echo "Error: kafka-topics.sh was not found and container 'kafka-lab' is not running." >&2
    echo "Start Kafka with: docker compose up -d" >&2
    exit 1
  fi
else
  echo "Error: kafka-topics.sh was not found." >&2
  echo "Install Kafka CLI, add it to PATH, or run the kafka-lab container with docker compose up -d." >&2
  exit 1
fi

echo "Topic creation command completed."
