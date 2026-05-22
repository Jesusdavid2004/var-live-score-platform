// =============================================================================
// match_state_service/src/rabbitmqPublisher.js — Puente Kafka → RabbitMQ
// =============================================================================
// Este módulo es el PUENTE entre el mundo de Kafka (eventos del partido) y
// el mundo de RabbitMQ (distribución en tiempo real al frontend y alertas).
//
// ¿Por qué existe este módulo separado?
// Separar la lógica de publicación de la lógica de consumo (index.js) y la
// lógica de estado (stateManager.js) sigue el principio de responsabilidad
// única: cada módulo hace UNA cosa bien.
//
// ¿Qué publica este módulo y a dónde?
//   publishScoreUpdate → Exchange "live_updates" (topic) → routing key "score.match_<id>"
//     → Cola "dashboard_q" (suscrita con patrón "score.*")
//     → dashboard_backend → WebSocket → navegadores
//
//   publishAlert → Exchange "live_alerts" (fanout) → routing key "" (ignorada)
//     → Cola "alerts_q" (enlazada al fanout)
//     → notification_backend → consola/push notifications
//
// Conexión RabbitMQ:
// La conexión se crea UNA SOLA VEZ en connect() y se mantiene en variables
// de módulo (channel, connection). Todas las llamadas a publish usan el mismo
// canal sin reconectar, lo que es mucho más eficiente.
// =============================================================================

// amqplib: librería cliente AMQP para Node.js (protocolo de RabbitMQ)
const amqplib = require("amqplib");

// rabbitmqUrl: URL de conexión leída desde la configuración centralizada
const { rabbitmqUrl } = require("./config");

// Variables de módulo que persisten entre llamadas a las funciones.
// Al ser variables del módulo (no locales de función), la conexión se reutiliza
// en todas las llamadas a publishScoreUpdate() y publishAlert().
let channel    = null;  // Canal AMQP: capa lógica sobre la conexión TCP
let connection = null;  // Conexión TCP con el broker de RabbitMQ

// Constantes con los nombres de exchanges para evitar errores de tipeo.
// Si el nombre cambia, solo hay que cambiarlo aquí, no en todas las llamadas.
const EXCHANGE_TOPIC  = "live_updates";     // Topic exchange → dashboard
const EXCHANGE_FANOUT = "live_alerts";      // Fanout exchange → notificaciones
const QUEUE_BETTING   = "betting_commands"; // Referencia a la cola de apuestas (no usada aquí directamente)

// =============================================================================
// FUNCIÓN ASYNC: connect()
// =============================================================================
// Establece la conexión con RabbitMQ y declara los exchanges necesarios.
// Se llama UNA SOLA VEZ al arrancar el servicio (en index.js).
//
// assertExchange es IDEMPOTENTE: si el exchange ya existe con los mismos
// parámetros, no falla. Esto es importante porque el dashboard_backend y
// el setup-exchanges.js también declaran los mismos exchanges al arrancar.
// RabbitMQ verifica que los parámetros coincidan y simplemente confirma.
// =============================================================================
async function connect() {
  // Establecemos la conexión TCP con el broker de RabbitMQ
  connection = await amqplib.connect(rabbitmqUrl);

  // Creamos un canal de comunicación sobre la conexión TCP.
  // Es más eficiente que abrir múltiples conexiones TCP.
  channel = await connection.createChannel();

  // Declaramos el Topic Exchange para actualizaciones de marcador.
  // type "topic": enruta mensajes a colas según patrones en la routing key.
  // durable: true → el exchange sobrevive reinicios del broker de RabbitMQ.
  await channel.assertExchange(EXCHANGE_TOPIC, "topic", { durable: true });

  // Declaramos el Fanout Exchange para alertas.
  // type "fanout": envía cada mensaje a TODAS las colas enlazadas, sin importar la routing key.
  // Ideal para notificaciones que deben llegar a múltiples sistemas simultáneamente.
  await channel.assertExchange(EXCHANGE_FANOUT, "fanout", { durable: true });

  console.log("[RABBIT] Exchanges declarados: live_updates (topic), live_alerts (fanout)");
}

// =============================================================================
// FUNCIÓN: publishScoreUpdate(matchId, state)
// =============================================================================
// Publica el marcador actualizado al Topic Exchange "live_updates".
// El dashboard_backend tiene su cola "dashboard_q" suscrita con el patrón
// "score.*", por lo que recibirá automáticamente este mensaje.
//
// Parámetros:
//   matchId → ID del partido (ej: "123")
//   state   → objeto con el estado: { match_id, home, away, event_type }
//
// ¿Por qué routing key "score.match_<matchId>"?
// El patrón permite filtrar por partido: si hubiera múltiples partidos
// simultáneos (score.match_123, score.match_456), el dashboard podría
// suscribirse solo a "score.match_123" o a "score.*" (todos los partidos).
// =============================================================================
function publishScoreUpdate(matchId, state) {
  // Construimos la routing key dinámica con el ID del partido
  const routingKey = `score.match_${matchId}`; // Ej: "score.match_123"

  // Serializamos el estado a JSON string para enviarlo como bytes
  const message = JSON.stringify(state);

  // channel.publish() envía el mensaje al exchange especificado con la routing key.
  // El exchange "live_updates" lo enruta a todas las colas con patrón coincidente.
  // Buffer.from() convierte el string a Buffer binario (formato requerido por AMQP).
  channel.publish(EXCHANGE_TOPIC, routingKey, Buffer.from(message));
  console.log(`[RABBIT] Publicado en ${EXCHANGE_TOPIC} | key=${routingKey}`, state);
}

// =============================================================================
// FUNCIÓN: publishAlert(message, severity)
// =============================================================================
// Publica una alerta al Fanout Exchange "live_alerts".
// Todos los servicios suscritos (actualmente solo notification_backend)
// recibirán este mensaje sin importar la routing key (el fanout la ignora).
//
// Parámetros:
//   message  → texto de la alerta (ej: "!GOL del equipo home!")
//   severity → nivel de importancia: "info" (gol) o "warning" (VAR/anulación)
//
// ¿Por qué usar un fanout exchange para alertas?
// Si en el futuro se agrega un servicio de SMS o email, solo necesita
// crear una cola y enlazarla al fanout exchange. No hay que cambiar
// este módulo ni el match_state_service: el broker se encarga del envío.
// =============================================================================
function publishAlert(message, severity) {
  // Construimos el payload de la alerta como objeto JSON
  const payload = JSON.stringify({ message, severity });

  // El segundo argumento "" es la routing key, que los fanout exchanges IGNORAN completamente.
  // Pasamos "" por convención, pero cualquier string serviría.
  channel.publish(EXCHANGE_FANOUT, "", Buffer.from(payload));
  console.log(`[RABBIT] Publicado en ${EXCHANGE_FANOUT} | severity=${severity}: ${message}`);
}

// =============================================================================
// FUNCIÓN ASYNC: close()
// =============================================================================
// Cierra limpiamente el canal y la conexión con RabbitMQ.
// Se llama cuando el proceso va a terminar para liberar recursos de red
// y evitar que el broker de RabbitMQ tenga conexiones zombie abiertas.
// =============================================================================
async function close() {
  if (channel) await channel.close();       // Cerramos el canal primero
  if (connection) await connection.close(); // Luego cerramos la conexión TCP
}

// Exportamos las funciones que otros módulos necesitan:
// - connect: llamada al arrancar el servicio (en index.js)
// - publishScoreUpdate: llamada por cada evento de Kafka procesado
// - publishAlert: llamada para eventos importantes (GOAL, VAR_CHECK, GOAL_ANNULLED)
// - close: llamada al terminar el proceso
module.exports = { connect, publishScoreUpdate, publishAlert, close };
