#!/bin/bash
set -e

echo "[kafka-init] Esperando a Kafka..."
sleep 10

TOPIC_NAME="match_events"
BOOTSTRAP_SERVER="kafka:9092"
PARTITIONS=3
REPLICATION_FACTOR=1

echo "[kafka-init] Creando topic ${TOPIC_NAME}..."
kafka-topics   --bootstrap-server "${BOOTSTRAP_SERVER}"   --create   --if-not-exists   --topic "${TOPIC_NAME}"   --partitions "${PARTITIONS}"   --replication-factor "${REPLICATION_FACTOR}"

echo "[kafka-init] Listando topics..."
kafka-topics --bootstrap-server "${BOOTSTRAP_SERVER}" --list

echo "[kafka-init] Topic creado correctamente."
