// ============================================================
// config.js — Configuración del match_state_service
// ============================================================
// Parámetros de conexión a Kafka y RabbitMQ para el servicio
// que mantiene el marcador del partido en tiempo real.
//
// Este servicio necesita AMBOS brokers:
//   - Kafka: para consumir eventos del partido (fuente de verdad)
//   - RabbitMQ: para publicar actualizaciones al dashboard
// Por eso tiene más variables que otros servicios.
// ============================================================

require("dotenv").config();

module.exports = {
  // Dirección del broker de Kafka (contenedor Docker o localhost en dev)
  kafkaBroker: process.env.KAFKA_BROKER || "kafka:9092",

  // Topic del que consume los eventos. Debe coincidir con el topic
  // creado por kafka-init y publicado por el live_feed_producer.
  kafkaTopic: process.env.KAFKA_TOPIC || "match_events",

  // Grupo de consumidores propio de este servicio.
  // Al tener su propio grupo, Kafka le entrega una copia de cada
  // mensaje independientemente de lo que lean otros servicios.
  stateGroupId: process.env.STATE_GROUP_ID || "state-group",

  // URL de conexión a RabbitMQ. El match_state_service publica
  // actualizaciones de marcador en el exchange "live_updates"
  // después de procesar cada evento de Kafka.
  rabbitmqUrl: process.env.RABBITMQ_URL || "amqp://guest:guest@rabbitmq:5672",
};
