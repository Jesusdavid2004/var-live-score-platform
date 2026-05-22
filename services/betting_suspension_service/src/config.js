// =============================================================================
// betting_suspension_service/src/config.js — Configuración del servicio de apuestas
// =============================================================================
// Centraliza los parámetros de conexión del betting_suspension_service.
// Al igual que el match_state_service, este servicio necesita AMBOS brokers:
//
//   KAFKA: para recibir eventos del partido y decidir cuándo suspender apuestas.
//     Lee del mismo topic "match_events" que los demás consumidores.
//
//   RABBITMQ: para publicar los comandos de suspensión/reanudación.
//     Escribe en la Work Queue "betting_commands" que procesa el betting_worker.
//
// ¿Por qué tiene su propio bettingGroupId?
// Cada servicio que consume Kafka necesita un Consumer Group único.
// Si este servicio y el match_state_service compartieran el mismo groupId,
// Kafka solo entregaría cada mensaje a UNO de ellos (balanceo de carga),
// y el otro nunca lo recibiría. Con groupIds distintos, ambos reciben todos.
// =============================================================================

// Carga las variables del archivo .env al objeto process.env
require("dotenv").config();

module.exports = {
  // ── Kafka ───────────────────────────────────────────────────────────────────

  // Dirección del broker de Kafka.
  // En Docker: "kafka:9092" (nombre del contenedor en docker-compose.yml)
  kafkaBroker: process.env.KAFKA_BROKER || "kafka:9092",

  // Nombre del topic de Kafka del que se consumen los eventos del partido.
  // Debe coincidir con el que usa el live_feed_producer al publicar.
  kafkaTopic: process.env.KAFKA_TOPIC || "match_events",

  // Consumer Group ID exclusivo de este servicio.
  // "betting-group" es diferente de "state-group" e "historical-archiver-group",
  // garantizando que Kafka entregue TODOS los mensajes a este servicio también,
  // sin interferir con los demás consumidores del mismo topic.
  bettingGroupId: process.env.BETTING_GROUP_ID || "betting-group",

  // ── RabbitMQ ────────────────────────────────────────────────────────────────

  // URL de RabbitMQ para publicar los comandos de apuestas.
  // Este servicio SOLO PUBLICA en RabbitMQ (no consume de RabbitMQ),
  // a diferencia del match_state_service que también publica.
  rabbitmqUrl: process.env.RABBITMQ_URL || "amqp://guest:guest@rabbitmq:5672",
};
