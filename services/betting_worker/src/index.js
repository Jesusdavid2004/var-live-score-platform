// ============================================================
// index.js — Punto de entrada del betting_worker
// ============================================================
// Este servicio implementa el patrón "Worker" o "Competing Consumer":
// consume comandos de la Work Queue "betting_commands" y ejecuta
// la acción correspondiente (suspender o reanudar apuestas).
//
// En un sistema real, aquí se llamaría a la API de la casa de
// apuestas para bloquear/desbloquear el mercado del partido.
// Para el ejercicio, se registra la acción en consola.
//
// Características del patrón Work Queue:
//   - Cada mensaje es procesado por exactamente UN worker
//   - Si hay varios workers, el broker balancea los mensajes
//   - prefetch(1): el worker no pide otro mensaje hasta terminar el actual
//   - noAck: false → reconocimiento manual (ACK/NACK)
// ============================================================

const amqplib = require("amqplib");
const { rabbitmqUrl } = require("./config");

// Nombre de la cola de la que consume este worker.
// Debe ser exactamente el mismo que usa el betting_suspension_service.
const QUEUE_BETTING = "betting_commands";

// ------------------------------------------------------------
// start()
// Conecta a RabbitMQ, declara la cola y arranca el consumer.
// El proceso queda corriendo indefinidamente esperando mensajes.
// ------------------------------------------------------------
async function start() {
  // Conectamos a RabbitMQ y creamos el canal de comunicación
  const connection = await amqplib.connect(rabbitmqUrl);
  const channel = await connection.createChannel();

  // Declaramos la cola (idempotente): si ya existe, no falla.
  // durable: true → la cola persiste aunque RabbitMQ se reinicie.
  await channel.assertQueue(QUEUE_BETTING, { durable: true });

  // prefetch(1): le dice a RabbitMQ que envíe solo 1 mensaje a la vez.
  // El worker no recibirá el siguiente hasta hacer ACK del actual.
  // Esto evita que un worker lento acumule muchos mensajes sin procesar.
  await channel.prefetch(1);

  console.log("[BETTING-WORKER] Esperando comandos en", QUEUE_BETTING);

  // Iniciamos el consumer con reconocimiento manual (noAck: false)
  channel.consume(
    QUEUE_BETTING,
    (msg) => {
      // Si msg es null, el consumer fue cancelado por el broker
      if (!msg) return;

      // Extraemos el contenido del mensaje como string
      const content = msg.content.toString();
      let command;

      try {
        // Parseamos el JSON del comando
        command = JSON.parse(content);
      } catch {
        // Si el mensaje está malformado, lo rechazamos sin re-encolar
        // (false, false = no re-encolar) para evitar un loop infinito
        console.error("[BETTING-WORKER] Error parseando comando:", content);
        channel.nack(msg, false, false);
        return;
      }

      // Elegimos el ícono según el tipo de comando para facilitar
      // la lectura visual de los logs en la consola
      const icon = command.command === "SUSPEND_BETS" ? "PAUSA" : "REANUDAR";

      // Registramos la acción. En producción aquí iría:
      //   await bettingApi.setMarketStatus(command.match_id, command.command)
      console.log(
        `[BETTING-WORKER] [${icon}] Comando: ${command.command} | match_id: ${command.match_id} | motivo: ${command.reason}`
      );

      // ACK manual: le confirmamos a RabbitMQ que el mensaje fue procesado.
      // Solo después de este ACK el broker lo elimina de la cola.
      // Si el worker muere antes del ACK, RabbitMQ re-encola el mensaje.
      channel.ack(msg);
    },
    { noAck: false } // Reconocimiento manual: esperamos el ACK explícito
  );
}

// Iniciamos el servicio. Si falla al conectar, terminamos con código 1
// para que Docker reinicie el contenedor automáticamente.
start().catch((err) => {
  console.error("[BETTING-WORKER] Error:", err);
  process.exit(1);
});
