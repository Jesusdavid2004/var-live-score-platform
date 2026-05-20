// ============================================================
// rabbitmqPublisher.js — Publicador de comandos de apuestas
// ============================================================
// Este módulo publica comandos (SUSPEND_BETS / RESUME_BETS)
// a la Work Queue "betting_commands" de RabbitMQ.
//
// ¿Por qué Work Queue y no un exchange?
// Una Work Queue garantiza que cada comando sea procesado por
// exactamente UN worker (patrón "competing consumers"). Si hay
// varios betting_workers corriendo, solo uno procesará cada
// comando, evitando duplicar la acción de suspender/reanudar.
// ============================================================

const amqplib = require("amqplib");
const { rabbitmqUrl } = require("./config");

// Variables de módulo para reutilizar la misma conexión
let channel = null;
let connection = null;

// Nombre de la Work Queue. Debe coincidir con el que escucha betting_worker.
const QUEUE_BETTING = "betting_commands";

// ------------------------------------------------------------
// connect()
// Conecta a RabbitMQ y declara la cola de comandos.
// assertQueue es idempotente: si la cola ya existe, no falla.
// durable: true → la cola sobrevive reinicios de RabbitMQ y
//                 los mensajes no se pierden.
// ------------------------------------------------------------
async function connect() {
  connection = await amqplib.connect(rabbitmqUrl);
  channel = await connection.createChannel();

  // Declaramos la cola antes de publicar para asegurarnos de que exista
  await channel.assertQueue(QUEUE_BETTING, { durable: true });

  console.log("[RABBIT] Queue declarada:", QUEUE_BETTING);
}

// ------------------------------------------------------------
// publishBettingCommand(command)
// Serializa el comando a JSON y lo envía a la Work Queue.
//
// persistent: true → el mensaje se guarda en disco en RabbitMQ.
// Así, si el broker se reinicia antes de que el worker procese
// el mensaje, el comando no se pierde.
// ------------------------------------------------------------
function publishBettingCommand(command) {
  const message = JSON.stringify(command);

  // sendToQueue publica directamente a la cola (sin pasar por exchange)
  // La opción persistent: true es el equivalente a durable en los mensajes
  channel.sendToQueue(QUEUE_BETTING, Buffer.from(message), { persistent: true });
  console.log("[RABBIT] Comando publicado a", QUEUE_BETTING, command);
}

// Cierra limpiamente la conexión cuando el proceso termina
async function close() {
  if (channel) await channel.close();
  if (connection) await connection.close();
}

module.exports = { connect, publishBettingCommand, close };
