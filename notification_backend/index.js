/**
 * notification_backend/index.js — Consumer de alertas en tiempo real
 * ===================================================================
 * Este servicio actúa como el sistema de notificaciones del proyecto.
 * Se suscribe al Fanout Exchange "live_alerts" de RabbitMQ y recibe
 * TODAS las alertas del partido: goles, revisiones VAR, goles anulados.
 *
 * En un sistema real de producción, aquí se enviarían:
 *   - Push notifications al celular de los usuarios
 *   - Correos electrónicos con resumen de goles
 *   - SMS a suscriptores premium
 *   - Mensajes a Slack/Teams para el equipo de operaciones
 *
 * Para el ejercicio académico: las alertas se muestran en consola
 * con formato visual claro y diferenciado por severidad.
 *
 * ¿Por qué un servicio separado para notificaciones?
 * Principio de responsabilidad única: el dashboard_backend maneja
 * la visualización del marcador; este servicio maneja las alertas.
 * Así se pueden escalar y desplegar de forma independiente.
 */

// amqplib: cliente RabbitMQ para Node.js
const amqp = require('amqplib');

// URL de RabbitMQ leída desde variable de entorno
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';

// Mapa de severidades a iconos visuales para la consola.
// Facilita identificar el tipo de alerta con un solo vistazo
// sin necesidad de leer el texto completo del mensaje.
const ICONOS = {
  info:    '⚽',  // Goles normales (evento positivo)
  warning: '🚨',  // VAR, goles anulados (evento de alerta)
  error:   '❌',  // Errores del sistema
  var:     '📺',  // Revisión VAR específica
};

/**
 * formatearAlerta(datos)
 * Convierte el objeto JSON de alerta en una línea de texto formateada
 * para mostrar en consola. Similar al formato de logs de producción.
 *
 * Formato de salida: "⚽ [PUSH][Partido 123] ¡GOL del equipo local!"
 *
 * @param {Object} datos - Objeto con { message, severity, match_id? }
 * @returns {string} Línea formateada para consola
 */
function formatearAlerta(datos) {
  // ICONOS[datos.severity] busca el icono por severidad;
  // si la severidad no está en el mapa, usa la campana genérica
  const icono  = ICONOS[datos.severity] || '🔔';

  // Si el mensaje incluye match_id, lo mostramos entre corchetes
  // para que sea fácil filtrar las alertas por partido
  const matchInfo = datos.match_id ? ` [Partido ${datos.match_id}]` : '';

  return `${icono} [PUSH]${matchInfo} ${datos.message}`;
}

/**
 * conectarRabbitMQ()
 * Establece la conexión con RabbitMQ y arranca el consumer de alertas.
 * Implementa reconexión automática en caso de fallo del broker.
 */
async function conectarRabbitMQ() {
  try {
    console.log('[NOTIF] Conectando a RabbitMQ...');
    const connection = await amqp.connect(RABBITMQ_URL);
    const channel    = await connection.createChannel();

    // Declaramos el Fanout Exchange (idempotente)
    // Un fanout entrega una copia del mensaje a TODAS las colas enlazadas
    await channel.assertExchange('live_alerts', 'fanout', { durable: true });

    // Cola durable: si el servicio se reinicia, los mensajes no procesados
    // siguen en la cola esperando, no se pierden
    const { queue } = await channel.assertQueue('alerts_q', { durable: true });

    // Enlazamos la cola al fanout exchange.
    // El segundo argumento '' es la routing key, ignorada en fanout.
    await channel.bindQueue(queue, 'live_alerts', '');

    console.log('[NOTIF] ✓ Suscrito a live_alerts → alerts_q');
    console.log('[NOTIF] Esperando alertas...\n');
    console.log('─'.repeat(50));

    // ── Consumer: procesa cada alerta que llega ──────────────────────────────
    channel.consume(queue, (msg) => {
      if (!msg) return;

      try {
        const contenido = msg.content.toString();
        const datos     = JSON.parse(contenido);

        // Formateamos el mensaje para mostrar en consola
        const linea = formatearAlerta(datos);

        // Agregamos separadores visuales para alertas importantes (VAR, anulaciones)
        // para que destaquen visualmente entre los goles normales
        if (datos.severity === 'warning' || datos.severity === 'var') {
          console.log('─'.repeat(50));
          console.log(linea); // La alerta importante va entre separadores
          console.log('─'.repeat(50));
        } else {
          // Goles normales y eventos informativos van sin separador
          console.log(linea);
        }

        // ACK: confirmamos que procesamos el mensaje correctamente
        channel.ack(msg);

      } catch (err) {
        console.error('[NOTIF] Error procesando alerta:', err.message);
        // NACK sin re-encolar para evitar loops infinitos con mensajes malformados
        channel.nack(msg, false, false);
      }
    });

    // Reconexión automática si el broker cierra la conexión inesperadamente
    connection.on('close', () => {
      console.warn('[NOTIF] Conexion cerrada. Reintentando en 5s...');
      setTimeout(conectarRabbitMQ, 5000);
    });

    connection.on('error', (err) => {
      console.error('[NOTIF] Error:', err.message);
    });

  } catch (error) {
    // Si RabbitMQ no está disponible todavía, reintentamos en 5 segundos
    console.error('[NOTIF] No se pudo conectar:', error.message);
    console.log('[NOTIF] Reintentando en 5 segundos...');
    setTimeout(conectarRabbitMQ, 5000);
  }
}

// ── Arranque del servicio ────────────────────────────────────────────────────
// Mostramos el banner de inicio antes de conectar a RabbitMQ
console.log('╔══════════════════════════════════════════════╗');
console.log('║     VAR Platform — Notification Backend      ║');
console.log('╚══════════════════════════════════════════════╝\n');

// Iniciamos la conexión a RabbitMQ y el loop de consumo
conectarRabbitMQ();
