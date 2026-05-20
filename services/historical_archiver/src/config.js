// ============================================================
// config.js — Configuración del historical_archiver
// ============================================================
// Centraliza los parámetros de conexión a Kafka para el servicio
// archivador. Usa variables de entorno para que el mismo código
// funcione tanto en Docker como en local.
//
// ¿Qué es el archiverGroupId?
// En Kafka, múltiples consumidores pueden leer el mismo topic
// si pertenecen a grupos distintos. El archiver tiene su propio
// grupo ("historical-archiver-group") para que Kafka le entregue
// todos los mensajes de forma independiente a los otros servicios
// (match_state_service, betting_suspension_service).
// ============================================================

// Carga las variables del archivo .env al objeto process.env
require("dotenv").config();

module.exports = {
  // Dirección del broker de Kafka. "kafka:9092" es el nombre del
  // contenedor Docker; en local se usaría "localhost:9092".
  kafkaBroker: process.env.KAFKA_BROKER || "kafka:9092",

  // Topic del que este servicio consume todos los eventos del partido.
  // Debe coincidir exactamente con el topic creado por kafka-init.
  kafkaTopic: process.env.KAFKA_TOPIC || "match_events",

  // ID del grupo de consumidores de este servicio.
  // Kafka usa el groupId para rastrear qué mensajes ya fueron
  // procesados (offset), permitiendo que el archiver retome
  // donde quedó si se reinicia.
  archiverGroupId:
    process.env.ARCHIVER_GROUP_ID || "historical-archiver-group",
};
