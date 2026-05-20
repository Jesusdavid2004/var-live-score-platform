// ============================================================
// config.js — Configuración del betting_suspension_service
// ============================================================
// Este servicio también necesita Kafka (para recibir eventos)
// y RabbitMQ (para enviar comandos de suspensión/reanudación
// a la cola betting_commands que procesa el betting_worker).
// ============================================================

require("dotenv").config();

module.exports = {
  // Broker de Kafka del que se consumen los eventos del partido
  kafkaBroker: process.env.KAFKA_BROKER || "kafka:9092",

  // Topic con los eventos del partido (mismo que usan los otros consumidores)
  kafkaTopic: process.env.KAFKA_TOPIC || "match_events",

  // Grupo de consumidores exclusivo de este servicio.
  // Al tener su propio grupo, recibe todos los mensajes de Kafka
  // sin interferir con el match_state_service o el archiver.
  bettingGroupId: process.env.BETTING_GROUP_ID || "betting-group",

  // URL de RabbitMQ para publicar comandos de apuestas
  rabbitmqUrl: process.env.RABBITMQ_URL || "amqp://guest:guest@rabbitmq:5672",
};
