// ============================================================
// config.js — Configuración centralizada del live_feed_producer
// ============================================================
// Este archivo es el único lugar donde se definen los parámetros
// de conexión y comportamiento del productor. Usa variables de
// entorno (.env) para que el mismo código funcione en local y
// en Docker sin cambiar nada: solo cambia el archivo .env.
//
// ¿Por qué variables de entorno?
// En sistemas distribuidos es una buena práctica separar la
// configuración del código (principio de 12-factor app). Así
// se puede cambiar el broker de Kafka sin tocar una sola línea
// de lógica.
// ============================================================

// Carga las variables definidas en el archivo .env al proceso
require("dotenv").config();

module.exports = {
  // Dirección del broker de Kafka al que se conecta el productor.
  // En Docker usa el nombre del contenedor "kafka"; en local usa localhost.
  kafkaBroker: process.env.KAFKA_BROKER || "kafka:9092",

  // Nombre del topic al que se publican todos los eventos del partido.
  // Debe coincidir con el topic creado por kafka-init en scripts/create-topics.sh.
  kafkaTopic: process.env.KAFKA_TOPIC || "match_events",

  // Identificador del cliente Kafka. Aparece en los logs del broker
  // y sirve para distinguir este productor de otros consumidores.
  kafkaClientId: process.env.KAFKA_CLIENT_ID || "live-feed-producer",

  // ID del partido simulado. Todos los eventos llevarán este match_id
  // para que los consumidores puedan filtrar por partido.
  matchId: process.env.MATCH_ID || "123",

  // Tiempo en milisegundos que el sistema espera antes de resolver el VAR.
  // Simula los ~20 segundos reales que tarda el árbitro en revisar.
  // Se convierte a número porque process.env siempre devuelve string.
  varDelayMs: Number(process.env.VAR_DELAY_MS || 20000),

  // URL de conexión a RabbitMQ. Se usa para suscribirse a la cola
  // restart_commands y recibir la señal de reinicio del partido.
  rabbitmqUrl: process.env.RABBITMQ_URL || "amqp://guest:guest@rabbitmq:5672",
};
