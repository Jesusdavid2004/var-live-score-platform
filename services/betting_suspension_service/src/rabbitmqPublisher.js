// =============================================================================
// betting_suspension_service/src/rabbitmqPublisher.js — Publicador de comandos
// =============================================================================
// Este módulo es la "salida" del betting_suspension_service: toma las
// decisiones de suspensión/reanudación y las publica como comandos en la
// Work Queue "betting_commands" de RabbitMQ para que el betting_worker las ejecute.
//
// ¿Por qué Work Queue y no un Exchange topic o fanout?
// Una Work Queue (cola de trabajo) implementa el patrón "Competing Consumers":
//   - Cada comando es procesado por EXACTAMENTE UN worker
//   - Si hay N betting_workers corriendo, RabbitMQ distribuye los comandos entre ellos
//   - Si un worker muere antes de hacer ACK, RabbitMQ re-entrega el mensaje al siguiente
//
// En contraste, un Fanout Exchange enviaría el mismo comando a TODOS los workers,
// causando que el partido se suspenda N veces (efecto duplicado no deseado).
//
// Flujo de este módulo:
//   betting_suspension_service evalúa evento → comando { SUSPEND_BETS / RESUME_BETS }
//   → publishBettingCommand() → sendToQueue("betting_commands") → betting_worker
// =============================================================================

// amqplib: librería cliente AMQP para Node.js (protocolo de RabbitMQ)
const amqplib = require("amqplib");

// rabbitmqUrl: URL de conexión leída desde la configuración
const { rabbitmqUrl } = require("./config");

// Variables de módulo para la conexión persistente.
// Se inicializan en connect() y se reutilizan en publishBettingCommand().
let channel    = null; // Canal AMQP: capa lógica sobre la conexión TCP
let connection = null; // Conexión TCP con el broker de RabbitMQ

// Nombre de la Work Queue donde se publican los comandos.
// DEBE ser exactamente el mismo que usa betting_worker al consumir.
// Una discrepancia en el nombre haría que los comandos nunca lleguen al worker.
const QUEUE_BETTING = "betting_commands";

// =============================================================================
// FUNCIÓN ASYNC: connect()
// =============================================================================
// Conecta a RabbitMQ y declara la cola "betting_commands".
// La declaración es idempotente: no falla si la cola ya existe (fue declarada
// por setup-exchanges.js u otro servicio al arrancar).
//
// ¿Por qué declarar la cola en el publicador y no solo en el consumidor?
// Es una buena práctica declarar la cola en ambos lados (publicador y consumidor)
// para garantizar que exista sin importar qué servicio arranca primero en Docker.
// =============================================================================
async function connect() {
  // Establecemos la conexión TCP con RabbitMQ
  connection = await amqplib.connect(rabbitmqUrl);

  // Creamos un canal de comunicación (más eficiente que múltiples conexiones)
  channel = await connection.createChannel();

  // Declaramos la Work Queue.
  // durable: true → la cola sobrevive reinicios de RabbitMQ.
  // Los mensajes en la cola no se pierden aunque el broker se reinicie.
  await channel.assertQueue(QUEUE_BETTING, { durable: true });

  console.log("[RABBIT] Queue declarada:", QUEUE_BETTING);
}

// =============================================================================
// FUNCIÓN: publishBettingCommand(command)
// =============================================================================
// Serializa el comando a JSON y lo envía directamente a la Work Queue.
//
// Parámetro:
//   command → objeto { command: "SUSPEND_BETS"|"RESUME_BETS", match_id, reason }
//
// ¿Qué significa persistent: true?
// Indica a RabbitMQ que el mensaje debe guardarse en DISCO (no solo en RAM).
// Si el broker se reinicia antes de que el betting_worker procese el mensaje,
// el comando sobrevive y el worker lo encontrará cuando se conecte.
// Sin persistent: true, el mensaje se perdería con cualquier reinicio del broker.
//
// ¿Por qué sendToQueue y no publish?
// sendToQueue publica directamente a la cola usando el exchange DEFAULT de RabbitMQ.
// channel.publish(exchange, routingKey, ...) envía a un exchange para enrutamiento.
// Como aquí no necesitamos enrutamiento (solo una cola destino), sendToQueue es más simple.
// =============================================================================
function publishBettingCommand(command) {
  const message = JSON.stringify(command); // Serializamos el comando a JSON string

  // Publicamos directamente a la cola de trabajo.
  // Buffer.from() convierte el string a bytes (formato binario requerido por AMQP).
  // persistent: true garantiza que el mensaje sobreviva reinicios del broker.
  channel.sendToQueue(QUEUE_BETTING, Buffer.from(message), { persistent: true });
  console.log("[RABBIT] Comando publicado a", QUEUE_BETTING, command);
}

// =============================================================================
// FUNCIÓN ASYNC: close()
// =============================================================================
// Cierra limpiamente el canal y la conexión al terminar el proceso.
// Libera los recursos de red y evita conexiones zombie en el broker.
// =============================================================================
async function close() {
  if (channel) await channel.close();       // Cerramos el canal lógico primero
  if (connection) await connection.close(); // Luego cerramos la conexión TCP
}

// Exportamos las funciones necesarias para el flujo del servicio:
// - connect: llamada al arrancar (en index.js)
// - publishBettingCommand: llamada por cada evento que requiere acción
// - close: llamada al terminar el proceso
module.exports = { connect, publishBettingCommand, close };
