// =============================================================================
// live_feed_producer/src/config.js — Configuración centralizada del productor
// =============================================================================
// Este archivo es el ÚNICO lugar donde se definen los parámetros de conexión
// y comportamiento del live_feed_producer. Centralizar la configuración aquí
// sigue el principio de "12-Factor App": separar la configuración del código.
//
// ¿Por qué variables de entorno?
// El mismo código debe funcionar en tres entornos sin cambiar una sola línea:
//   - Desarrollo local: KAFKA_BROKER=localhost:29092, RABBITMQ_URL=localhost:5672
//   - Docker: KAFKA_BROKER=kafka:9092, RABBITMQ_URL=amqp://guest:guest@rabbitmq:5672
//   - Producción: KAFKA_BROKER=kafka.miempresa.com:9092 (con autenticación)
// Solo cambia el archivo .env, nunca el código fuente.
//
// ¿Cómo se cargan las variables?
// require("dotenv").config() lee el archivo .env de la raíz del proyecto y
// carga cada línea como una propiedad de process.env. Por ejemplo:
//   .env:         MATCH_ID=456
//   process.env:  process.env.MATCH_ID === "456"
// =============================================================================

// dotenv: carga las variables del archivo .env al objeto global process.env
// Debe llamarse ANTES de leer cualquier process.env para que estén disponibles
require("dotenv").config();

module.exports = {
  // ── Kafka ───────────────────────────────────────────────────────────────────
  // Dirección del broker de Kafka al que se conecta el productor.
  // En Docker: "kafka:9092" (nombre del servicio en docker-compose.yml)
  // En local:  "localhost:29092" (el puerto externo mapeado en docker-compose.yml)
  kafkaBroker: process.env.KAFKA_BROKER || "kafka:9092",

  // Nombre del topic de Kafka donde se publican todos los eventos del partido.
  // Todos los consumidores (match_state_service, historical_archiver,
  // betting_suspension_service) deben usar exactamente este mismo nombre.
  // El topic debe existir antes de publicar (lo crea kafka-init al arrancar).
  kafkaTopic: process.env.KAFKA_TOPIC || "match_events",

  // Identificador único del cliente Kafka. Aparece en los logs del broker
  // y en el panel de administración de Kafka (Kafka UI) para identificar
  // qué cliente envió cada mensaje. Útil para debugging en producción.
  kafkaClientId: process.env.KAFKA_CLIENT_ID || "live-feed-producer",

  // ── Partido ─────────────────────────────────────────────────────────────────
  // ID del partido simulado. Todos los eventos generados llevarán este match_id
  // para que los consumidores puedan filtrar mensajes por partido.
  // En un sistema real con múltiples partidos simultáneos, este valor
  // cambiaría dinámicamente para cada instancia del productor.
  matchId: process.env.MATCH_ID || "123",

  // Tiempo de espera (en ms) antes de resolver la revisión VAR.
  // Simula los segundos de tensión mientras el árbitro revisa las imágenes.
  // Valor por defecto: 20000ms = 20 segundos (tiempo real aproximado del VAR).
  // Number() convierte el string de process.env a número: "20000" → 20000
  varDelayMs: Number(process.env.VAR_DELAY_MS || 20000),

  // ── RabbitMQ ────────────────────────────────────────────────────────────────
  // URL completa de conexión a RabbitMQ con usuario, contraseña, host y puerto.
  // El productor usa RabbitMQ EXCLUSIVAMENTE para escuchar la cola
  // "restart_commands" y recibir la señal de reinicio del dashboard.
  // (A diferencia del match_state_service, este servicio NO publica en RabbitMQ,
  // solo consume la señal de reinicio.)
  rabbitmqUrl: process.env.RABBITMQ_URL || "amqp://guest:guest@rabbitmq:5672",
};
