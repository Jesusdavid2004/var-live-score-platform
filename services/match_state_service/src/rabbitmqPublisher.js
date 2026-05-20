// ============================================================
// rabbitmqPublisher.js — Publicador RabbitMQ del match_state_service
// ============================================================
// Este módulo actúa como puente entre Kafka y RabbitMQ.
// Después de que stateManager.js procesa un evento de Kafka,
// este módulo publica el resultado (marcador actualizado o alerta)
// en RabbitMQ para que llegue al dashboard y a las notificaciones.
//
// Exchanges que usa:
//   1. "live_updates" (topic)  → actualización de marcador al dashboard
//   2. "live_alerts"  (fanout) → alertas de gol/VAR a las notificaciones
// ============================================================

const amqplib = require("amqplib");
const { rabbitmqUrl } = require("./config");

// Variables de módulo que guardan la conexión y el canal activos.
// Al ser variables de módulo (no locales), la conexión persiste
// entre llamadas a publishScoreUpdate() y publishAlert().
let channel = null;
let connection = null;

// Nombres de exchanges/colas. Constantes para evitar errores de tipeo.
const EXCHANGE_TOPIC  = "live_updates";   // Topic exchange para el dashboard
const EXCHANGE_FANOUT = "live_alerts";    // Fanout exchange para notificaciones
const QUEUE_BETTING   = "betting_commands"; // Cola de comandos de apuestas (referencia)

// ------------------------------------------------------------
// connect()
// Establece la conexión con RabbitMQ y declara los exchanges.
// Se llama una sola vez al arrancar el servicio (en index.js).
//
// assertExchange es idempotente: si el exchange ya existe con
// los mismos parámetros, no falla. Esto es importante porque
// otros servicios (dashboard_backend) también los declaran.
// ------------------------------------------------------------
async function connect() {
  connection = await amqplib.connect(rabbitmqUrl);
  channel = await connection.createChannel();

  // Declaramos ambos exchanges. durable: true significa que
  // sobreviven reinicios del broker de RabbitMQ.
  await channel.assertExchange(EXCHANGE_TOPIC, "topic", { durable: true });
  await channel.assertExchange(EXCHANGE_FANOUT, "fanout", { durable: true });

  console.log("[RABBIT] Exchanges declarados: live_updates (topic), live_alerts (fanout)");
}

// ------------------------------------------------------------
// publishScoreUpdate(matchId, state)
// Publica el marcador actualizado al Topic Exchange "live_updates".
//
// Routing key: "score.match_<matchId>" (ej: "score.match_123")
// El dashboard_backend tiene una cola suscrita al patrón "score.*",
// así que recibirá este mensaje automáticamente.
//
// El objeto "state" contiene: { match_id, home, away, event_type }
// ------------------------------------------------------------
function publishScoreUpdate(matchId, state) {
  const routingKey = `score.match_${matchId}`; // Clave de routing del topic exchange
  const message = JSON.stringify(state);        // Serializamos el estado a JSON

  // publish() envía el mensaje al exchange con la routing key indicada.
  // El exchange se encarga de enrutarlo a las colas suscritas.
  channel.publish(EXCHANGE_TOPIC, routingKey, Buffer.from(message));
  console.log(`[RABBIT] Publicado en ${EXCHANGE_TOPIC} | key=${routingKey}`, state);
}

// ------------------------------------------------------------
// publishAlert(message, severity)
// Publica una alerta al Fanout Exchange "live_alerts".
//
// Un fanout exchange envía el mensaje a TODAS las colas enlazadas,
// sin importar la routing key (por eso se pasa "" vacía).
// El notification_backend tiene su cola "alerts_q" enlazada aquí.
//
// severity: "info" (gol normal), "warning" (VAR/gol anulado)
// ------------------------------------------------------------
function publishAlert(message, severity) {
  const payload = JSON.stringify({ message, severity });

  // El segundo argumento "" es la routing key, ignorada en fanout
  channel.publish(EXCHANGE_FANOUT, "", Buffer.from(payload));
  console.log(`[RABBIT] Publicado en ${EXCHANGE_FANOUT} | severity=${severity}: ${message}`);
}

// ------------------------------------------------------------
// close()
// Cierra limpiamente la conexión con RabbitMQ.
// Se llama cuando el proceso va a terminar para liberar recursos.
// ------------------------------------------------------------
async function close() {
  if (channel) await channel.close();
  if (connection) await connection.close();
}

module.exports = { connect, publishScoreUpdate, publishAlert, close };
