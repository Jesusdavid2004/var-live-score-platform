// =============================================================================
// betting_worker/src/index.js — Worker de ejecución de comandos de apuestas
// =============================================================================
// Este servicio implementa el patrón "Worker" (también llamado "Consumer Worker"
// o "Task Consumer"). Su único propósito es recibir comandos de la Work Queue
// "betting_commands" y ejecutar la acción correspondiente.
//
// ¿Cuál es la diferencia entre este servicio y el betting_suspension_service?
//   - betting_suspension_service: DECIDE cuándo actuar (lee Kafka, evalúa reglas)
//   - betting_worker: EJECUTA la acción (lee RabbitMQ, llama a la API de apuestas)
//
// Esta separación sigue el patrón Command: quien genera el comando (suspension_service)
// no necesita saber cómo se ejecuta (worker). Ventajas:
//   - Se pueden tener múltiples workers en paralelo para mayor throughput
//   - El suspension_service no se bloquea esperando que la API de apuestas responda
//   - Si la API de apuestas está lenta, los comandos se acumulan en la cola
//     y se procesan cuando el sistema se recupere (sin perder ninguno)
//
// Características del patrón Work Queue:
//   - prefetch(1): el worker pide un mensaje a la vez (no acumula)
//   - noAck: false → reconocimiento manual (ACK explícito después de procesar)
//   - Si el worker muere antes del ACK, RabbitMQ re-entrega el mensaje a otro worker
//
// En producción aquí se llamaría a la API de la plataforma de apuestas:
//   await bettingAPI.setMarketStatus(command.match_id, "SUSPENDED")
//   await bettingAPI.setMarketStatus(command.match_id, "ACTIVE")
// =============================================================================

// amqplib: cliente de RabbitMQ para Node.js
const amqplib = require("amqplib");

// rabbitmqUrl: URL de conexión a RabbitMQ desde la configuración del servicio
const { rabbitmqUrl } = require("./config");

// Nombre de la Work Queue de la que consume este worker.
// DEBE coincidir exactamente con el nombre usado por betting_suspension_service
// al publicar los comandos. Una discrepancia haría que los comandos nunca lleguen.
const QUEUE_BETTING = "betting_commands";

// =============================================================================
// FUNCIÓN ASYNC: start()
// =============================================================================
// Conecta a RabbitMQ, declara la cola, configura el prefetch y arranca
// el consumer. El proceso queda corriendo indefinidamente procesando comandos.
//
// ¿Por qué no hay reconexión automática aquí?
// En este ejercicio académico, si RabbitMQ se cae el proceso termina y
// Docker lo reinicia (según la restart policy del docker-compose.yml).
// En producción se implementaría reconexión automática similar a los otros servicios.
// =============================================================================
async function start() {
  // Establecemos la conexión TCP con el broker de RabbitMQ
  const connection = await amqplib.connect(rabbitmqUrl);

  // Creamos el canal de comunicación sobre la conexión
  const channel = await connection.createChannel();

  // Declaramos la cola (idempotente: no falla si ya existe).
  // durable: true → la cola y sus mensajes persisten ante reinicios del broker.
  await channel.assertQueue(QUEUE_BETTING, { durable: true });

  // prefetch(1): le decimos a RabbitMQ que envíe MÁXIMO 1 mensaje a la vez.
  // El worker no recibirá el siguiente mensaje hasta que haga ACK del actual.
  //
  // ¿Por qué es importante prefetch(1)?
  // Sin prefetch, RabbitMQ podría enviar todos los mensajes pendientes al worker
  // de golpe. Si el worker muere mientras procesa el mensaje 50 (de 100 enviados),
  // los mensajes 51-100 que ya estaban "en vuelo" pero sin ACK también se perderían.
  // Con prefetch(1), solo un mensaje está "en vuelo" a la vez, minimizando la pérdida.
  await channel.prefetch(1);

  console.log("[BETTING-WORKER] Esperando comandos en", QUEUE_BETTING);

  // Iniciamos el consumer con reconocimiento MANUAL (noAck: false).
  // Esto significa que RabbitMQ no eliminará el mensaje de la cola hasta
  // que este worker llame explícitamente a channel.ack(msg).
  channel.consume(
    QUEUE_BETTING,
    (msg) => {
      // null indica que el consumer fue cancelado por el broker (ej: cola borrada)
      if (!msg) return;

      // Extraemos el contenido del mensaje como string de texto
      const content = msg.content.toString();
      let command;

      try {
        // Parseamos el JSON del comando de apuestas
        command = JSON.parse(content);
      } catch {
        // Mensaje malformado (no es JSON válido): lo rechazamos sin re-encolar.
        // Re-encolar un mensaje malformado causaría un loop infinito de errores
        // porque ningún worker podría procesarlo.
        console.error("[BETTING-WORKER] Error parseando comando:", content);
        channel.nack(msg, false, false); // false, false = no re-encolar
        return;
      }

      // Elegimos el texto del ícono según el tipo de comando para que los logs
      // sean más legibles visualmente: "PAUSA" para suspender, "REANUDAR" para activar.
      const icon = command.command === "SUSPEND_BETS" ? "PAUSA" : "REANUDAR";

      // Registramos la acción ejecutada.
      // En producción esta línea sería reemplazada por una llamada real a la API:
      //   const result = await bettingApi.setMarketStatus(command.match_id, command.command);
      console.log(
        `[BETTING-WORKER] [${icon}] Comando: ${command.command} | match_id: ${command.match_id} | motivo: ${command.reason}`
      );

      // ACK manual: le confirmamos a RabbitMQ que el mensaje fue procesado con éxito.
      // SOLO DESPUÉS de este ACK, RabbitMQ elimina el mensaje de la cola definitivamente.
      // Si el worker muriera antes de llegar aquí, RabbitMQ re-entregaría el mensaje
      // a otro worker disponible (garantía de "at-least-once delivery").
      channel.ack(msg);
    },
    { noAck: false } // Reconocimiento manual: NO confirmación automática al recibir
  );
}

// Iniciamos el servicio.
// .catch() maneja errores de conexión al arrancar (ej: RabbitMQ no disponible).
// process.exit(1) hace que Docker reinicie el contenedor automáticamente.
start().catch((err) => {
  console.error("[BETTING-WORKER] Error:", err);
  process.exit(1); // Código 1 = error → Docker intentará reiniciar según restart policy
});
