// =============================================================================
// dashboard_backend/setup-exchanges.js — Declaración de la topología RabbitMQ
// =============================================================================
// Este script define TODA la infraestructura de mensajería RabbitMQ que
// necesita el sistema VAR Live Score Platform. Se puede ejecutar una sola
// vez antes de levantar los servicios, o al inicio de cada servicio.
//
// ¿Por qué existe este archivo separado?
// En sistemas distribuidos, la topología del broker de mensajería (exchanges,
// colas, bindings) debe estar definida antes de que los servicios intenten
// publicar o consumir mensajes. Este script centraliza esa configuración
// para que sea fácil de entender y modificar sin tocar la lógica de negocio.
//
// ¿Qué es idempotente?
// Idempotente significa que puedes ejecutar este script N veces y el resultado
// siempre es el mismo: RabbitMQ no falla si el exchange o la cola ya existen
// con los mismos parámetros. Es una propiedad importante en Docker donde
// varios contenedores pueden intentar declarar los mismos recursos al arrancar.
//
// Topología que crea este script:
//   1. Topic Exchange  "live_updates"     → routing key "score.*" → dashboard_q
//   2. Fanout Exchange "live_alerts"      → todas las colas enlazadas → alerts_q
//   3. Work Queue      "betting_commands" → (sin exchange, cola directa)
//
// Relación con los demás servicios:
//   - match_state_service PUBLICA en "live_updates" y "live_alerts"
//   - dashboard_backend   CONSUME de "dashboard_q"  (enlazada a "live_updates")
//   - notification_backend CONSUME de "alerts_q"    (enlazada a "live_alerts")
//   - betting_suspension_service PUBLICA en "betting_commands"
//   - betting_worker      CONSUME de "betting_commands"
// =============================================================================

// amqplib: librería cliente de RabbitMQ para Node.js
// Implementa el protocolo AMQP 0-9-1 que usa RabbitMQ internamente
const amqp = require('amqplib');

// URL de conexión leída desde variable de entorno para flexibilidad entre
// entornos (Docker usa "rabbitmq" como hostname; local usa "localhost")
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';

// =============================================================================
// FUNCIÓN ASYNC: setupExchanges()
// =============================================================================
// Conecta a RabbitMQ y declara todos los exchanges, colas y bindings del sistema.
// Si algo falla, registra el error y termina el proceso con código 1.
// =============================================================================
async function setupExchanges() {
  let connection; // Variable en scope exterior para poder cerrarla en el catch
  try {
    console.log('[SETUP] Conectando a RabbitMQ...');

    // Establecemos la conexión TCP con el broker de RabbitMQ
    connection = await amqp.connect(RABBITMQ_URL);

    // Creamos un canal de comunicación sobre la conexión TCP.
    // Un canal es un "tubo virtual" dentro de la misma conexión: es más eficiente
    // crear múltiples canales que múltiples conexiones TCP completas.
    const channel = await connection.createChannel();

    // ─────────────────────────────────────────────────────────────────────────
    // 1. TOPIC EXCHANGE: "live_updates"
    // ─────────────────────────────────────────────────────────────────────────
    // Un Topic Exchange es como un "enrutador inteligente": recibe mensajes y
    // los distribuye a las colas según si la routing key del mensaje coincide
    // con el patrón de binding de cada cola.
    //
    // Ejemplo de flujo:
    //   match_state_service publica con key "score.match_123"
    //   → el exchange compara con el patrón "score.*" de dashboard_q
    //   → el patrón coincide (score.algo) → entrega a dashboard_q
    //
    // durable: true → el exchange sobrevive si RabbitMQ se reinicia,
    //                  evitando que se pierda la configuración.
    // ─────────────────────────────────────────────────────────────────────────
    await channel.assertExchange('live_updates', 'topic', {
      durable: true, // El exchange persiste aunque el broker se reinicie
    });
    console.log('[SETUP] ✓ Exchange "live_updates" (topic) declarado');

    // Declaramos la cola del dashboard donde llegarán las actualizaciones de marcador.
    // durable: true → la cola y sus mensajes persisten ante reinicios de RabbitMQ
    const { queue: dashboardQueue } = await channel.assertQueue('dashboard_q', {
      durable: true,
    });

    // Enlazamos (binding) la cola al exchange con el patrón "score.*".
    // El símbolo * en RabbitMQ Topic matching = exactamente UNA palabra.
    // "score.*" coincide con: "score.123", "score.match_abc", etc.
    // "score.#" coincidiría con: "score.123.extra.palabras" (# = cero o más palabras)
    await channel.bindQueue(dashboardQueue, 'live_updates', 'score.*');
    console.log('[SETUP] ✓ Cola "dashboard_q" enlazada con binding "score.*"');

    // ─────────────────────────────────────────────────────────────────────────
    // 2. FANOUT EXCHANGE: "live_alerts"
    // ─────────────────────────────────────────────────────────────────────────
    // Un Fanout Exchange es un "broadcast": envía CADA mensaje a TODAS las
    // colas que estén enlazadas a él, sin importar la routing key.
    // Es perfecto para notificaciones que deben llegar a múltiples sistemas:
    //   - Sistema de email para enviar resúmenes de goles a suscriptores
    //   - Sistema de push notifications para celulares
    //   - Sistema de SMS para alertas premium
    //   - En este ejercicio: el notification_backend que imprime en consola
    // ─────────────────────────────────────────────────────────────────────────
    await channel.assertExchange('live_alerts', 'fanout', {
      durable: true, // También persiste ante reinicios del broker
    });
    console.log('[SETUP] ✓ Exchange "live_alerts" (fanout) declarado');

    // Cola de alertas que consume el notification_backend
    const { queue: alertsQueue } = await channel.assertQueue('alerts_q', {
      durable: true,
    });

    // En un fanout exchange, la routing key del binding se ignora completamente.
    // Pasamos '' (string vacío) por convención, pero cualquier valor serviría.
    await channel.bindQueue(alertsQueue, 'live_alerts', '');
    console.log('[SETUP] ✓ Cola "alerts_q" enlazada al fanout');

    // ─────────────────────────────────────────────────────────────────────────
    // 3. WORK QUEUE: "betting_commands"
    // ─────────────────────────────────────────────────────────────────────────
    // Una Work Queue (también llamada "Task Queue") implementa el patrón
    // "Competing Consumers" o "Round Robin":
    //   - Si hay 1 worker: procesa todos los mensajes secuencialmente
    //   - Si hay N workers: cada mensaje va a exactamente UNO de los workers
    //   - RabbitMQ hace el balanceo de carga automáticamente
    //
    // ¿Por qué no usar un exchange? Las Work Queues usan el exchange DEFAULT
    // de RabbitMQ (string vacío '') que enruta directamente a la cola por nombre.
    // Es el patrón más simple y eficiente para colas de tareas.
    //
    // En este sistema:
    //   - PUBLICADOR: betting_suspension_service (envía SUSPEND_BETS/RESUME_BETS)
    //   - CONSUMIDOR: betting_worker (ejecuta la acción de suspender/reanudar)
    // ─────────────────────────────────────────────────────────────────────────
    await channel.assertQueue('betting_commands', {
      durable: true, // Los comandos no se pierden si el broker se reinicia
    });
    console.log('[SETUP] ✓ Work Queue "betting_commands" declarada');

    // Imprimimos un resumen de toda la topología creada para verificación visual
    console.log('\n[SETUP] ════════════════════════════════════════');
    console.log('[SETUP]  Topologia RabbitMQ lista:');
    console.log('[SETUP]  Exchange  live_updates  (topic)  → dashboard_q [score.*]');
    console.log('[SETUP]  Exchange  live_alerts   (fanout) → alerts_q    [*]');
    console.log('[SETUP]  Queue     betting_commands       (work queue)');
    console.log('[SETUP] ════════════════════════════════════════');
    console.log('[SETUP]  Panel de gestion: http://localhost:15672');
    console.log('[SETUP]  Usuario: guest | Password: guest');
    console.log('[SETUP] ════════════════════════════════════════\n');

    // Cerramos el canal y la conexión limpiamente.
    // Este script solo declara la topología y termina: los servicios
    // se encargan de sus propias conexiones en tiempo de ejecución.
    await channel.close();
    await connection.close();
    console.log('[SETUP] Conexion cerrada limpiamente. Setup completo.');

  } catch (error) {
    console.error('[SETUP] ERROR:', error.message);
    // Cerramos la conexión aunque haya error para no dejar recursos colgados
    if (connection) await connection.close().catch(() => {});
    // Terminamos con código 1 para que Docker detecte el fallo y pueda reintentar
    process.exit(1);
  }
}

// Ejecutamos el setup inmediatamente cuando se corre este script.
// Node.js ejecuta el módulo de arriba a abajo, y al llegar aquí
// lanza setupExchanges() y el proceso termina cuando la Promise resuelve.
setupExchanges();
