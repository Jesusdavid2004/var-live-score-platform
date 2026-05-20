/**
 * setup-exchanges.js — Declaración de la topología RabbitMQ
 * ==========================================================
 * Este script define TODA la infraestructura de mensajería
 * RabbitMQ que necesita el sistema. Se puede ejecutar una sola
 * vez antes de levantar los servicios, o al inicio de cada
 * servicio (es idempotente: RabbitMQ no falla si los exchanges
 * y colas ya existen con los mismos parámetros).
 *
 * Crea tres canales de comunicación:
 *   1. Topic Exchange  "live_updates"     → marcador al dashboard
 *   2. Fanout Exchange "live_alerts"      → alertas a notificaciones
 *   3. Work Queue      "betting_commands" → comandos de apuestas
 *
 * IDEMPOTENTE: se puede llamar N veces sin efectos secundarios.
 * Esto es importante en Docker donde varios contenedores pueden
 * intentar declarar los mismos recursos al arrancar.
 */

// amqplib es la librería cliente de RabbitMQ para Node.js
const amqp = require('amqplib');

// URL de conexión leída desde variable de entorno (o valor por defecto para local)
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';

async function setupExchanges() {
  let connection;
  try {
    console.log('[SETUP] Conectando a RabbitMQ...');
    connection = await amqp.connect(RABBITMQ_URL);

    // Creamos un canal de comunicación sobre la conexión TCP.
    // Un canal es un "tubo virtual" dentro de la conexión; es más
    // eficiente crear varios canales que varias conexiones TCP.
    const channel = await connection.createChannel();

    // ─────────────────────────────────────────────────────────
    // 1. TOPIC EXCHANGE: "live_updates"
    // ─────────────────────────────────────────────────────────
    // Un Topic Exchange enruta mensajes a las colas según un
    // patrón en la routing key. El dashboard usa el patrón
    // "score.*" para recibir actualizaciones de CUALQUIER partido
    // (score.123, score.456, etc.) sin cambiar su código.
    //
    // durable: true → el exchange sobrevive reinicios del broker.
    // ─────────────────────────────────────────────────────────
    await channel.assertExchange('live_updates', 'topic', {
      durable: true,
    });
    console.log('[SETUP] ✓ Exchange "live_updates" (topic) declarado');

    // Cola del dashboard: recibe todos los mensajes cuya routing key
    // empiece con "score." seguido de cualquier palabra (ej: score.123)
    const { queue: dashboardQueue } = await channel.assertQueue('dashboard_q', {
      durable: true, // La cola persiste aunque RabbitMQ se reinicie
    });
    // Enlazamos la cola al exchange con el patrón "score.*"
    // El * en RabbitMQ topic matches exactamente UNA palabra
    await channel.bindQueue(dashboardQueue, 'live_updates', 'score.*');
    console.log('[SETUP] ✓ Cola "dashboard_q" enlazada con binding "score.*"');

    // ─────────────────────────────────────────────────────────
    // 2. FANOUT EXCHANGE: "live_alerts"
    // ─────────────────────────────────────────────────────────
    // Un Fanout Exchange envía CADA mensaje a TODAS las colas
    // enlazadas, ignorando la routing key. Es perfecto para
    // notificaciones que deben llegar a múltiples consumidores
    // simultáneamente (dashboard, email, SMS, push, etc.)
    // ─────────────────────────────────────────────────────────
    await channel.assertExchange('live_alerts', 'fanout', {
      durable: true,
    });
    console.log('[SETUP] ✓ Exchange "live_alerts" (fanout) declarado');

    // Cola de alertas enlazada al fanout sin routing key (no aplica en fanout)
    const { queue: alertsQueue } = await channel.assertQueue('alerts_q', {
      durable: true,
    });
    // El segundo argumento '' es la routing key, ignorada por los fanout exchanges
    await channel.bindQueue(alertsQueue, 'live_alerts', '');
    console.log('[SETUP] ✓ Cola "alerts_q" enlazada al fanout');

    // ─────────────────────────────────────────────────────────
    // 3. WORK QUEUE: "betting_commands"
    // ─────────────────────────────────────────────────────────
    // Una Work Queue (cola de trabajo) implementa el patrón
    // "competing consumers": si hay N workers escuchando, cada
    // mensaje lo procesa exactamente UNO. RabbitMQ hace el
    // balanceo de carga automáticamente entre los workers.
    //
    // No necesita exchange explícito: se publica directamente
    // a la cola usando el exchange por defecto de RabbitMQ.
    // ─────────────────────────────────────────────────────────
    await channel.assertQueue('betting_commands', {
      durable: true, // Los mensajes no se pierden si el broker se reinicia
    });
    console.log('[SETUP] ✓ Work Queue "betting_commands" declarada');

    // Resumen de la topología creada para verificación visual
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
    // Este script termina su trabajo aquí: solo declara la topología.
    await channel.close();
    await connection.close();
    console.log('[SETUP] Conexion cerrada limpiamente. Setup completo.');

  } catch (error) {
    console.error('[SETUP] ERROR:', error.message);
    // Cerramos la conexión aunque haya error para no dejarla colgada
    if (connection) await connection.close().catch(() => {});
    // Terminamos con código de error para que Docker lo detecte
    process.exit(1);
  }
}

// Ejecutamos el setup inmediatamente al correr el script
setupExchanges();
