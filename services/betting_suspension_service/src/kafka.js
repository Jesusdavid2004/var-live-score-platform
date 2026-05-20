// ============================================================
// kafka.js — Cliente Kafka del betting_suspension_service
// ============================================================
// Crea el consumer de KafkaJS para el servicio de suspensión
// de apuestas. Usa "betting-group" como groupId para que Kafka
// lo trate como un consumidor independiente del match_state_service
// aunque ambos lean el mismo topic.
// ============================================================

const { Kafka, logLevel } = require("kafkajs");
const { kafkaBroker, bettingGroupId } = require("./config");

// clientId "betting-suspension-service" identifica este consumer
// en los logs del broker y en el panel de administración de Kafka.
const kafka = new Kafka({
  clientId: "betting-suspension-service",
  brokers: [kafkaBroker],
  logLevel: logLevel.INFO,
});

// Cada grupo de consumidores en Kafka es independiente:
// el broker lleva un offset separado para "betting-group"
// y para "state-group", así ambos reciben todos los mensajes.
const consumer = kafka.consumer({ groupId: bettingGroupId });

module.exports = { consumer };
