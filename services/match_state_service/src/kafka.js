// ============================================================
// kafka.js — Cliente Kafka del match_state_service (consumidor)
// ============================================================
// Crea el consumer de KafkaJS para el servicio de estado del
// partido. Usa su propio groupId para recibir todos los mensajes
// del topic de forma independiente a otros servicios.
// ============================================================

const { Kafka, logLevel } = require("kafkajs");
const { kafkaBroker, stateGroupId } = require("./config");

// El clientId "match-state-service" identifica este consumer
// en los logs del broker de Kafka, facilitando el monitoreo.
const kafka = new Kafka({
  clientId: "match-state-service",
  brokers: [kafkaBroker],
  logLevel: logLevel.INFO,
});

// Consumer con groupId "state-group".
// Kafka recuerda el offset (posición) de este grupo, así que
// si el servicio se reinicia, retoma desde donde quedó sin
// reprocesar mensajes ya procesados.
const consumer = kafka.consumer({ groupId: stateGroupId });

module.exports = { consumer };
