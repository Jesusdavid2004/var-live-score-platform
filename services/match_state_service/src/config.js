// =============================================================================
// match_state_service/src/config.js — Configuración del árbitro digital
// =============================================================================
// Centraliza TODOS los parámetros de conexión del match_state_service.
// Este servicio es especial porque necesita DOS brokers de mensajería:
//
//   - KAFKA: fuente de verdad de los eventos del partido.
//     Consume del topic "match_events" para recibir cada KICKOFF, GOAL, etc.
//
//   - RABBITMQ: canal de distribución hacia el frontend y las notificaciones.
//     Publica en "live_updates" (marcador al dashboard) y en "live_alerts" (alertas).
//
// Esta dualidad refleja el patrón arquitectónico del sistema:
//   Kafka = backbone de eventos (alta durabilidad, replay, múltiples consumidores)
//   RabbitMQ = distribución en tiempo real (baja latencia, routing flexible)
//
// ¿Por qué no usar solo Kafka para todo?
// Kafka es excelente para almacenar y distribuir eventos entre servicios backend,
// pero RabbitMQ tiene mejor soporte para patrones como Fanout (broadcast a
// múltiples colas) y es más sencillo de integrar con WebSocket backends.
// =============================================================================

// dotenv carga el archivo .env y sus variables quedan en process.env
require("dotenv").config();

module.exports = {
  // ── Kafka ───────────────────────────────────────────────────────────────────

  // Dirección del broker de Kafka.
  // "kafka:9092" es el hostname del contenedor Docker definido en docker-compose.yml.
  // En desarrollo local sin Docker se usaría "localhost:29092" (puerto externo).
  kafkaBroker: process.env.KAFKA_BROKER || "kafka:9092",

  // Topic de Kafka del que se consumen los eventos.
  // IMPORTANTE: debe ser exactamente el mismo nombre que usa el live_feed_producer
  // al publicar y el que fue creado por kafka-init en scripts/create-topics.sh.
  // Una discrepancia en el nombre haría que este servicio nunca reciba mensajes.
  kafkaTopic: process.env.KAFKA_TOPIC || "match_events",

  // ID del Consumer Group de Kafka para este servicio.
  // Al tener su propio grupo ("state-group"), Kafka garantiza que este servicio
  // reciba TODOS los mensajes del topic, independientemente de lo que lean
  // el historical_archiver ("historical-archiver-group") o el betting service.
  stateGroupId: process.env.STATE_GROUP_ID || "state-group",

  // ── RabbitMQ ────────────────────────────────────────────────────────────────

  // URL completa de conexión a RabbitMQ con credenciales y host.
  // Este servicio PUBLICA en dos exchanges de RabbitMQ:
  //   1. "live_updates" (topic exchange): marcador actualizado → dashboard_backend
  //   2. "live_alerts"  (fanout exchange): alertas → notification_backend
  rabbitmqUrl: process.env.RABBITMQ_URL || "amqp://guest:guest@rabbitmq:5672",
};
