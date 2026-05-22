// =============================================================================
// live_feed_producer/src/index.js — Punto de entrada del productor de eventos
// =============================================================================
// Este servicio es el "generador de eventos" del sistema: simula lo que en
// un sistema real sería una fuente de datos del partido en vivo (sensores
// GPS, cámaras, APIs de federaciones deportivas, etc.).
//
// ¿Qué hace exactamente?
//   1. Se conecta a Kafka y ejecuta el escenario del partido UNA SOLA VEZ
//   2. Al terminar, se queda ESCUCHANDO la cola RabbitMQ "restart_commands"
//   3. Cuando llega el mensaje { command: "RESTART" }, vuelve a correr
//      el partido desde el inicio (KICKOFF)
//   4. Este ciclo se repite indefinidamente hasta que el contenedor se detenga
//
// ¿Por qué esperar una señal en lugar de loop automático?
// Para que el botón "Reiniciar Partido" del dashboard tenga control real:
// el partido solo vuelve a empezar cuando el USUARIO lo decide, no de forma
// automática. Esto permite hacer demostraciones controladas en clase.
//
// Posición en la arquitectura:
//   ESTE SERVICIO → Kafka (match_events) → match_state_service, historical_archiver,
//                                           betting_suspension_service
//   dashboard_backend → RabbitMQ (restart_commands) → ESTE SERVICIO (señal de reinicio)
// =============================================================================

// amqplib: cliente de RabbitMQ para suscribirse a la cola de comandos de reinicio
const amqp    = require("amqplib");

// producer: instancia del productor Kafka, creada en kafka.js como singleton
// Un singleton garantiza que solo haya una conexión activa con el broker de Kafka
const { producer } = require("./kafka");

// runScenario: función que ejecuta la secuencia completa de eventos del partido
// (KICKOFF → FOUL → GOAL → VAR_CHECK → GOAL_ANNULLED → MATCH_END)
const { runScenario } = require("./scenario");

// sleep: función utilitaria que pausa la ejecución por N milisegundos
// La usamos para esperar entre reintentos de conexión a RabbitMQ
const { sleep } = require("./utils");

// rabbitmqUrl: URL de conexión a RabbitMQ leída desde variables de entorno
const { rabbitmqUrl } = require("./config");

// Nombre de la cola de RabbitMQ donde llegan los comandos de reinicio.
// Esta constante debe coincidir EXACTAMENTE con la que usa el dashboard_backend
// en su endpoint POST /reiniciar, de lo contrario los mensajes no llegarían.
const RESTART_QUEUE = "restart_commands";

// =============================================================================
// FUNCIÓN ASYNC: esperarReinicio()
// =============================================================================
// Esta función BLOQUEA la ejecución del programa hasta que llega el mensaje
// { "command": "RESTART" } en la cola "restart_commands" de RabbitMQ.
//
// ¿Cómo funciona el bloqueo?
// Usa una Promise que solo se resuelve (resolve()) cuando recibe el mensaje
// correcto. Mientras tanto, el proceso queda "pausado" en el await de esta
// función sin consumir CPU, solo manteniendo la conexión abierta.
//
// ¿Qué pasa si RabbitMQ se cae mientras espera?
// El bloque try/catch detecta el error, cierra la conexión colgada,
// espera 5 segundos (para no saturar el broker) y vuelve a intentarlo.
// Este bucle while(true) interno garantiza que nunca se rinda.
// =============================================================================
async function esperarReinicio() {
  // El while(true) externo maneja la reconexión a RabbitMQ si la conexión se pierde
  while (true) {
    let conn; // Variable declarada en scope externo para poder cerrarla en el catch
    try {
      // Establecemos la conexión TCP con el broker de RabbitMQ
      conn = await amqp.connect(rabbitmqUrl);

      // Creamos el canal de comunicación sobre la conexión
      const ch = await conn.createChannel();

      // Declaramos la cola de reinicio (idempotente: no falla si ya existe).
      // durable: true → la cola sobrevive reinicios del broker, los mensajes no se pierden.
      await ch.assertQueue(RESTART_QUEUE, { durable: true });

      console.log("[PRODUCER] Esperando señal de reinicio en restart_commands...");

      // Usamos una Promise para convertir el callback de RabbitMQ en código async.
      // La Promise se resuelve cuando llega el mensaje RESTART, o se rechaza
      // si la conexión se cierra inesperadamente.
      await new Promise((resolve, reject) => {
        // Si la conexión se cierra mientras esperamos, rechazamos la Promise
        // para que el catch externo maneje la reconexión
        conn.on("close", () => reject(new Error("RabbitMQ conexion cerrada")));
        conn.on("error", reject); // También rechazamos ante errores de red

        // Registramos el consumer: se ejecuta por cada mensaje que llega
        ch.consume(RESTART_QUEUE, (msg) => {
          if (!msg) return; // null si el consumer fue cancelado por el broker

          try {
            // Parseamos el mensaje JSON: esperamos { command: "RESTART" }
            const data = JSON.parse(msg.content.toString());

            if (data.command === "RESTART") {
              // Recibimos la señal de reinicio: hacemos ACK y resolvemos la Promise
              // para que el código que llamó a esperarReinicio() pueda continuar
              ch.ack(msg);
              resolve(); // Esto "desbloquea" el await esperarReinicio() en start()
            } else {
              // Si llega otro comando desconocido, lo descartamos con ACK
              // (no lo re-encolamos para no acumular basura en la cola)
              ch.ack(msg);
            }
          } catch (_) {
            // Mensaje malformado: NACK sin re-encolar para evitar loops
            ch.nack(msg, false, false);
          }
        });
      });

      // Si llegamos aquí, recibimos RESTART exitosamente.
      // Cerramos la conexión a RabbitMQ limpiamente antes de correr el partido.
      await ch.close().catch(() => {}); // .catch() evita error si ya estaba cerrado
      await conn.close().catch(() => {});
      return; // Salimos de esperarReinicio(): el partido puede empezar

    } catch (err) {
      // Error de conexión (RabbitMQ caído, red cortada, etc.):
      // cerramos la conexión colgada y reintentamos en 5 segundos
      console.error("[PRODUCER] Error esperando reinicio:", err.message);
      if (conn) await conn.close().catch(() => {});
      console.log("[PRODUCER] Reintentando conexión a RabbitMQ en 5s...");
      await sleep(5000); // Pausa antes de reintentar (evita saturar el broker)
    }
  }
}

// =============================================================================
// FUNCIÓN ASYNC: start()
// =============================================================================
// Orquesta el ciclo de vida completo del productor:
//   1. Conecta a Kafka (una sola vez al inicio)
//   2. Corre el primer partido al arrancar el servicio
//   3. Espera señal RESTART de RabbitMQ indefinidamente
//   4. Al recibir RESTART, corre el partido de nuevo
//   5. Vuelve al paso 3 (bucle infinito controlado por señales externas)
//
// ¿Por qué conectar Kafka solo una vez y no dentro del bucle?
// Conectar y desconectar Kafka en cada partido sería costoso (handshake TCP,
// negociación de protocolo, etc.). Mejor mantener la conexión abierta y
// reutilizarla para todos los partidos.
//
// El bloque try/catch/finally garantiza que la conexión a Kafka se cierre
// limpiamente aunque ocurra un error inesperado, liberando recursos del broker.
// =============================================================================
async function start() {
  try {
    // Paso 1: Conectar al broker de Kafka.
    // Si Kafka no está disponible (aún iniciando en Docker), lanza una excepción
    // que es capturada por el catch y termina el proceso (Docker lo reiniciará).
    await producer.connect();
    console.log("[PRODUCER] Conectado a Kafka");

    // Paso 2: Ejecutar el primer partido inmediatamente al arrancar el contenedor.
    // No esperamos señal para el primer partido: el servicio debe mostrar
    // contenido desde el inicio sin que nadie tenga que presionar el botón.
    console.log("[PRODUCER] ── Iniciando primer partido ──");
    await runScenario(); // Ejecuta KICKOFF → FOUL → GOAL → VAR → GOAL_ANNULLED → MATCH_END
    console.log("[PRODUCER] Partido finalizado. Esperando reinicio...");

    // Paso 3+: Bucle infinito que alterna entre esperar señal y correr partido.
    // Este while(true) nunca termina por sí solo: solo se interrumpe si
    // Docker detiene el contenedor o si ocurre un error no capturado.
    while (true) {
      // Bloqueamos aquí hasta que el dashboard_backend publique { command: "RESTART" }
      await esperarReinicio();

      // Llegamos aquí solo después de recibir la señal de reinicio
      console.log("[PRODUCER] ── Reiniciando partido ──");
      await runScenario(); // Nuevo partido completo desde KICKOFF
      console.log("[PRODUCER] Partido finalizado. Esperando reinicio...");
    }

  } catch (error) {
    // Error fatal (ej: Kafka no disponible al arrancar):
    // lo registramos para que aparezca en los logs de Docker
    console.error("[PRODUCER] Error fatal:", error);

  } finally {
    // La cláusula finally se ejecuta SIEMPRE, haya error o no.
    // Desconectamos el producer de Kafka limpiamente para liberar
    // los recursos de red y evitar conexiones zombie en el broker.
    await producer.disconnect();
    console.log("[PRODUCER] Desconectado de Kafka");
  }
}

// Iniciamos el servicio ejecutando start().
// Nota: start() es async y retorna una Promise, pero no necesitamos
// hacer .catch() aquí porque el try/catch interno maneja todos los errores.
start();
