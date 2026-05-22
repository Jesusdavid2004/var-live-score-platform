// =============================================================================
// betting_worker/src/config.js — Configuración del worker de apuestas
// =============================================================================
// El betting_worker es el servicio más simple de la arquitectura en términos
// de configuración: solo necesita conectarse a RabbitMQ.
//
// ¿Por qué solo RabbitMQ y no Kafka?
// En la arquitectura del sistema, el betting_worker NO escucha Kafka directamente.
// El flujo completo es:
//
//   Kafka → betting_suspension_service → RabbitMQ (betting_commands) → betting_worker
//
// El betting_suspension_service actúa como intermediario:
//   1. Lee Kafka (eventos del partido)
//   2. Evalúa las reglas de negocio
//   3. Publica el comando en RabbitMQ
//   4. El betting_worker ejecuta ese comando
//
// Esta separación de responsabilidades permite:
//   - Escalar los workers independientemente del suspension service
//   - Agregar más workers si hay muchos comandos sin tocar el suspension service
//   - El suspension service no se bloquea esperando la API de apuestas
// =============================================================================

// dotenv: carga las variables del archivo .env al objeto process.env
require("dotenv").config();

module.exports = {
  // URL completa de conexión a RabbitMQ.
  // El worker CONSUME de la Work Queue "betting_commands".
  // Esta URL le dice al worker dónde encontrar el broker donde está esa cola.
  // En Docker: "amqp://guest:guest@rabbitmq:5672" (hostname del contenedor)
  // En local:  "amqp://guest:guest@localhost:5672"
  rabbitmqUrl: process.env.RABBITMQ_URL || "amqp://guest:guest@rabbitmq:5672",
};
