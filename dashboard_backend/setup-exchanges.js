/**
 * setup-exchanges.js
 * ==================
 * T3.1 — Declara toda la topologia RabbitMQ del sistema.
 *
 * Este script crea:
 *   1. Topic Exchange  "live_updates"  → para marcadores (dashboard)
 *   2. Fanout Exchange "live_alerts"   → para alertas (notifications)
 *   3. Work Queue      "betting_commands" → para apuestas (de P2)
 *
 * Se puede correr solo una vez antes de levantar los servicios,
 * o incluirlo dentro de cada servicio al arrancar (idempotente).
 *
 * IDEMPOTENTE: RabbitMQ no falla si el exchange ya existe
 * con los mismos parametros — se puede llamar N veces sin problema.
 */

const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';

async function setupExchanges() {
  let connection;
  try {
    console.log('[SETUP] Conectando a RabbitMQ...');
    connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();

    // ─────────────────────────────────────────────────────────
    // 1. TOPIC EXCHANGE: live_updates
    //    Routing key pattern: "score.<match_id>"
    //    El dashboard se suscribe a "score.*" para recibir
    //    actualizaciones de cualquier partido.
    // ─────────────────────────────────────────────────────────
    await channel.assertExchange('live_updates', 'topic', {
      durable: true,   // sobrevive reinicios de RabbitMQ
    });
    console.log('[SETUP] ✓ Exchange "live_updates" (topic) declarado');

    // Cola del dashboard suscrita a todos los partidos
    const { queue: dashboardQueue } = await channel.assertQueue('dashboard_q', {
      durable: true,
    });
    await channel.bindQueue(dashboardQueue, 'live_updates', 'score.*');
    console.log('[SETUP] ✓ Cola "dashboard_q" enlazada con binding "score.*"');

    // ─────────────────────────────────────────────────────────
    // 2. FANOUT EXCHANGE: live_alerts
    //    Todos los consumers reciben TODOS los mensajes.
    //    Usado para alertas VAR, goles, etc.
    // ─────────────────────────────────────────────────────────
    await channel.assertExchange('live_alerts', 'fanout', {
      durable: true,
    });
    console.log('[SETUP] ✓ Exchange "live_alerts" (fanout) declarado');

    // Cola de alertas — se enlaza sin routing key (fanout ignora la key)
    const { queue: alertsQueue } = await channel.assertQueue('alerts_q', {
      durable: true,
    });
    await channel.bindQueue(alertsQueue, 'live_alerts', '');
    console.log('[SETUP] ✓ Cola "alerts_q" enlazada al fanout');

    // ─────────────────────────────────────────────────────────
    // 3. WORK QUEUE: betting_commands
    //    Patron "competing consumers": un solo worker procesa
    //    cada mensaje (no hay duplicados).
    //    durable: true → los mensajes sobreviven reinicios.
    //    prefetch 1    → un mensaje a la vez por worker.
    // ─────────────────────────────────────────────────────────
    await channel.assertQueue('betting_commands', {
      durable: true,
    });
    console.log('[SETUP] ✓ Work Queue "betting_commands" declarada');

    // ─────────────────────────────────────────────────────────
    // Resumen final para verificacion en el panel :15672
    // ─────────────────────────────────────────────────────────
    console.log('\n[SETUP] ════════════════════════════════════════');
    console.log('[SETUP]  Topologia RabbitMQ lista:');
    console.log('[SETUP]  Exchange  live_updates  (topic)  → dashboard_q [score.*]');
    console.log('[SETUP]  Exchange  live_alerts   (fanout) → alerts_q    [*]');
    console.log('[SETUP]  Queue     betting_commands       (work queue)');
    console.log('[SETUP] ════════════════════════════════════════');
    console.log('[SETUP]  Panel de gestion: http://localhost:15672');
    console.log('[SETUP]  Usuario: guest | Password: guest');
    console.log('[SETUP] ════════════════════════════════════════\n');

    await channel.close();
    await connection.close();
    console.log('[SETUP] Conexion cerrada limpiamente. Setup completo.');

  } catch (error) {
    console.error('[SETUP] ERROR:', error.message);
    if (connection) await connection.close().catch(() => {});
    process.exit(1);
  }
}

setupExchanges();
