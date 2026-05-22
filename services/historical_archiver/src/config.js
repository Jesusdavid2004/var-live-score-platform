// =============================================================================
// historical_archiver/src/config.js — Configuración del archivador histórico
// =============================================================================
// Centraliza los parámetros de conexión del historical_archiver.
// A diferencia del match_state_service y el betting_suspension_service,
// este servicio SOLO necesita Kafka porque únicamente CONSUME eventos
// y los persiste. No publica en RabbitMQ ni en ningún otro broker.
//
// ¿Por qué el archiver solo tiene Kafka y no RabbitMQ?
// El historical_archiver es un "sink" puro en la arquitectura de datos:
// solo recibe información y la almacena, sin producir ni distribuir nada.
// No necesita RabbitMQ porque su salida es la base de datos (o consola en
// este ejercicio), no un mensaje a otro servicio.
//
// Comparación de dependencias de los servicios del sistema:
//   live_feed_producer:          Kafka(write) + RabbitMQ(read  restart_commands)
//   match_state_service:         Kafka(read)  + RabbitMQ(write live_updates/live_alerts)
//   betting_suspension_service:  Kafka(read)  + RabbitMQ(write betting_commands)
//   betting_worker:              solo RabbitMQ(read  betting_commands)
//   historical_archiver:         solo Kafka(read)     ← el más simple
//   dashboard_backend:           solo RabbitMQ(read+write)
//   notification_backend:        solo RabbitMQ(read)
// =============================================================================

// dotenv: carga las variables del archivo .env al objeto global process.env
require("dotenv").config();

module.exports = {
  // Dirección del broker de Kafka.
  // "kafka:9092" es el hostname del contenedor Docker definido en docker-compose.yml.
  // Cuando los contenedores están en la misma red Docker, se comunican por nombre.
  kafkaBroker: process.env.KAFKA_BROKER || "kafka:9092",

  // Nombre del topic de Kafka del que se consumen todos los eventos del partido.
  // Es el mismo topic que usan los demás servicios: todos leen del mismo lugar.
  // IMPORTANTE: si este nombre difiere del que crea kafka-init, el archiver
  // no recibiría ningún mensaje (consumiría de un topic vacío o inexistente).
  kafkaTopic: process.env.KAFKA_TOPIC || "match_events",

  // ID del Consumer Group exclusivo del archivador.
  // Al usar "historical-archiver-group", Kafka mantiene un offset completamente
  // independiente del resto de servicios. Esto garantiza que el archivador
  // archive TODOS los eventos sin saltarse ninguno, incluso si los otros
  // servicios (match_state, betting) ya los procesaron hace tiempo.
  archiverGroupId:
    process.env.ARCHIVER_GROUP_ID || "historical-archiver-group",
};
