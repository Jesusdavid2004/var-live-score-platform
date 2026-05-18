/**
 * notification_backend/index.js
 * ==============================
 * T3.3 — Consumer del Fanout Exchange "live_alerts".
 *
 * Recibe TODAS las alertas del sistema (goles, VAR, suspensiones)
 * y las imprime en consola con formato claro.
 *
 * En un sistema real, aqui iria la logica de push notifications,
 * emails, SMS, etc. Para el ejercicio: consola formateada.
 */

const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';

// Mapa de severidades a emojis para la consola
const ICONOS = {
  info:    '⚽',
  warning: '🚨',
  error:   '❌',
  var:     '📺',
};

/**
 * formatearAlerta — convierte el JSON de alerta en una linea
 * de consola legible, igual que los logs de produccion reales.
 */
function formatearAlerta(datos) {
  const ahora  = new Date().toISOString();
  const icono  = ICONOS[datos.severity] || '🔔';
  const matchInfo = datos.match_id ? ` [Partido ${datos.match_id}]` : '';

  return `${icono} [PUSH]${matchInfo} ${datos.message}`;
}

async function conectarRabbitMQ() {
  try {
    console.log('[NOTIF] Conectando a RabbitMQ...');
    const connection = await amqp.connect(RABBITMQ_URL);
    const channel    = await connection.createChannel();

    // Fanout exchange — declaramos idempotente
    await channel.assertExchange('live_alerts', 'fanout', { durable: true });

    // Cola durable para no perder alertas si el servicio se reinicia
    const { queue } = await channel.assertQueue('alerts_q', { durable: true });

    // Enlazamos la cola al fanout (sin routing key — recibe todo)
    await channel.bindQueue(queue, 'live_alerts', '');

    console.log('[NOTIF] ✓ Suscrito a live_alerts → alerts_q');
    console.log('[NOTIF] Esperando alertas...\n');
    console.log('─'.repeat(50));

    channel.consume(queue, (msg) => {
      if (!msg) return;

      try {
        const contenido = msg.content.toString();
        const datos     = JSON.parse(contenido);
        const linea     = formatearAlerta(datos);

        // Separador visual segun severidad
        if (datos.severity === 'warning' || datos.severity === 'var') {
          console.log('─'.repeat(50));
          console.log(linea);
          console.log('─'.repeat(50));
        } else {
          console.log(linea);
        }

        channel.ack(msg);

      } catch (err) {
        console.error('[NOTIF] Error procesando alerta:', err.message);
        channel.nack(msg, false, false);
      }
    });

    connection.on('close', () => {
      console.warn('[NOTIF] Conexion cerrada. Reintentando en 5s...');
      setTimeout(conectarRabbitMQ, 5000);
    });

    connection.on('error', (err) => {
      console.error('[NOTIF] Error:', err.message);
    });

  } catch (error) {
    console.error('[NOTIF] No se pudo conectar:', error.message);
    console.log('[NOTIF] Reintentando en 5 segundos...');
    setTimeout(conectarRabbitMQ, 5000);
  }
}

console.log('╔══════════════════════════════════════════════╗');
console.log('║     VAR Platform — Notification Backend      ║');
console.log('╚══════════════════════════════════════════════╝\n');
conectarRabbitMQ();
