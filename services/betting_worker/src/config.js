// ============================================================
// config.js — Configuración del betting_worker
// ============================================================
// El betting_worker solo necesita conectarse a RabbitMQ porque
// su única función es consumir comandos de la Work Queue
// "betting_commands". No interactúa directamente con Kafka.
//
// ¿Por qué solo RabbitMQ?
// En la arquitectura del sistema, el betting_suspension_service
// actúa como intermediario entre Kafka y RabbitMQ. El worker
// solo necesita escuchar la cola final de comandos.
// ============================================================

require("dotenv").config();

module.exports = {
  // URL de conexión a RabbitMQ para consumir la cola de comandos
  rabbitmqUrl: process.env.RABBITMQ_URL || "amqp://guest:guest@rabbitmq:5672",
};
