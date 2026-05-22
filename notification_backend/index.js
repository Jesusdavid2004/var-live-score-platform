// =============================================================================
// notification_backend/index.js — Consumer de alertas en tiempo real
// =============================================================================
// Este servicio es el sistema de notificaciones del proyecto VAR Live Score.
// Se suscribe al Fanout Exchange "live_alerts" de RabbitMQ y recibe TODAS
// las alertas importantes del partido: goles, revisiones VAR, goles anulados.
//
// ¿Por qué existe un servicio separado para notificaciones?
// Principio de Responsabilidad Única (SRP): el dashboard_backend se encarga
// de la visualización del marcador; este servicio se encarga de las alertas.
// Al estar separados, se pueden escalar de forma independiente:
//   - Si hay muchos usuarios en el dashboard → escalar dashboard_backend
//   - Si hay muchos suscriptores de notificaciones → escalar notification_backend
//
// ¿Qué hace con las alertas recibidas?
// En este ejercicio académico: las imprime en consola con formato visual.
// En un sistema real de producción, aquí se enviarían:
//   - Push notifications al celular de los usuarios (Firebase, APNs)
//   - Correos electrónicos con resumen del gol (SendGrid, SES)
//   - SMS a suscriptores premium (Twilio, AWS SNS)
//   - Mensajes a Slack/Teams para el equipo de operaciones
//
// Posición en la arquitectura:
//   match_state_service → RabbitMQ (live_alerts fanout) → ESTE SERVICIO → consola/notificaciones
//
// Puerto: 3004 (no expone HTTP, solo consume RabbitMQ)
// =============================================================================

// amqplib: cliente oficial de RabbitMQ para Node.js
// Implementa el protocolo AMQP 0-9-1 para conectarse al broker
const amqp = require('amqplib');

// URL de RabbitMQ leída desde variable de entorno (.env).
// En Docker: "amqp://guest:guest@rabbitmq:5672"
// En local:  "amqp://guest:guest@localhost:5672"
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';

// =============================================================================
// MAPA DE ICONOS POR SEVERIDAD
// =============================================================================
// Cada alerta tiene una severidad que indica su importancia.
// Usamos emojis para hacer los logs más legibles de un vistazo:
//   info    → goles normales (evento positivo esperado)
//   warning → VAR, goles anulados (evento que cambia el estado del partido)
//   error   → errores del sistema (algo salió mal)
//   var     → revisión VAR específica (subcategoría de warning)
// =============================================================================
const ICONOS = {
  info:    '⚽',   // Gol normal: evento positivo
  warning: '🚨',   // Gol anulado / VAR: evento de alta importancia
  error:   '❌',   // Error del sistema
  var:     '📺',   // Revisión VAR: el árbitro de video está actuando
};

// =============================================================================
// FUNCIÓN: formatearAlerta(datos)
// =============================================================================
// Convierte el objeto JSON de alerta en una línea de texto formateada y
// lista para mostrar en consola. Sirve como plantilla de salida consistente.
//
// Formato de salida ejemplo:
//   "⚽ [PUSH][Partido 123] ¡GOL del equipo home!"
//   "🚨 [PUSH][Partido 123] ¡GOL ANULADO POR EL VAR!"
//
// Parámetro:
//   datos → objeto con campos { message, severity, match_id? }
//
// ¿Por qué separar el formateo de la impresión?
// Separar la lógica de formateo del I/O (consola) facilita los tests unitarios
// y permite cambiar la salida (ej: escribir a un archivo de log) sin tocar
// la lógica de cómo se construye el mensaje.
// =============================================================================
function formatearAlerta(datos) {
  // Buscamos el icono correspondiente a la severidad del mensaje.
  // Si la severidad no está en el mapa (ej: severidad nueva), usamos la campana genérica.
  const icono  = ICONOS[datos.severity] || '🔔';

  // Si el mensaje incluye el ID del partido, lo mostramos entre corchetes
  // para facilitar el filtrado cuando hay múltiples partidos simultáneos.
  const matchInfo = datos.match_id ? ` [Partido ${datos.match_id}]` : '';

  // Construimos la línea final: icono + tag [PUSH] + partido + mensaje
  return `${icono} [PUSH]${matchInfo} ${datos.message}`;
}

// =============================================================================
// FUNCIÓN ASYNC: conectarRabbitMQ()
// =============================================================================
// Establece la conexión con RabbitMQ, se suscribe al Fanout Exchange
// "live_alerts" y procesa cada alerta que llega.
//
// ¿Por qué tiene reconexión automática?
// En Docker, los servicios no siempre arrancan en orden. RabbitMQ puede tardar
// unos segundos. La reconexión automática evita que este servicio muera
// si RabbitMQ no está listo cuando arranca notification_backend.
//
// Flujo de datos:
//   match_state_service → publica en "live_alerts" → cola "alerts_q"
//   → esta función consume → formatearAlerta() → consola
// =============================================================================
async function conectarRabbitMQ() {
  try {
    console.log('[NOTIF] Conectando a RabbitMQ...');

    // Establecemos la conexión TCP con el broker
    const connection = await amqp.connect(RABBITMQ_URL);

    // Creamos el canal de comunicación sobre la conexión.
    // Cada operación AMQP (publish, consume, ack) se hace a través del canal.
    const channel    = await connection.createChannel();

    // Declaramos el Fanout Exchange (idempotente).
    // Un fanout entrega UNA COPIA del mensaje a TODAS las colas enlazadas.
    // durable: true → el exchange sobrevive reinicios del broker.
    await channel.assertExchange('live_alerts', 'fanout', { durable: true });

    // Declaramos la cola durable "alerts_q".
    // durable: true → si RabbitMQ se reinicia, los mensajes no procesados
    // permanecen en la cola esperando, no se pierden.
    const { queue } = await channel.assertQueue('alerts_q', { durable: true });

    // Enlazamos la cola al fanout exchange.
    // La routing key '' es ignorada en fanout: todos los mensajes llegan aquí.
    // Es el equivalente a "subscribirse" al canal de alertas del partido.
    await channel.bindQueue(queue, 'live_alerts', '');

    console.log('[NOTIF] ✓ Suscrito a live_alerts → alerts_q');
    console.log('[NOTIF] Esperando alertas...\n');
    console.log('─'.repeat(50)); // Separador visual para distinguir alertas del log de arranque

    // ── Consumer: se ejecuta por cada alerta que llega ────────────────────────
    // channel.consume() registra el callback que RabbitMQ invocará automáticamente
    // cada vez que llegue un mensaje a la cola "alerts_q".
    channel.consume(queue, (msg) => {
      if (!msg) return; // null si el consumer fue cancelado por el broker

      try {
        const contenido = msg.content.toString(); // Buffer binario → string de texto
        const datos     = JSON.parse(contenido);  // string JSON → objeto JavaScript

        // Construimos la línea de texto formateada para mostrar en consola
        const linea = formatearAlerta(datos);

        // Las alertas importantes (warning/var) van enmarcadas entre separadores
        // para que destaquen visualmente en el flujo de logs de la consola
        if (datos.severity === 'warning' || datos.severity === 'var') {
          console.log('─'.repeat(50));
          console.log(linea); // La alerta crítica va entre las líneas de separación
          console.log('─'.repeat(50));
        } else {
          // Las alertas informativas (goles normales) van sin separador
          console.log(linea);
        }

        // ACK manual: confirmamos a RabbitMQ que el mensaje fue procesado.
        // Solo después de este ACK el broker lo elimina de la cola.
        // Si el servicio muere antes del ACK, RabbitMQ re-entregará el mensaje.
        channel.ack(msg);

      } catch (err) {
        console.error('[NOTIF] Error procesando alerta:', err.message);
        // NACK (false, false): rechazamos el mensaje sin re-encolarlo.
        // Re-encolar un mensaje malformado causaría un loop infinito de errores.
        channel.nack(msg, false, false);
      }
    });

    // Si la conexión se cierra inesperadamente (RabbitMQ reiniciado, red cortada),
    // esperamos 5 segundos y volvemos a conectar desde cero.
    connection.on('close', () => {
      console.warn('[NOTIF] Conexion cerrada. Reintentando en 5s...');
      setTimeout(conectarRabbitMQ, 5000);
    });

    // Registramos errores de conexión para diagnóstico (no terminan el proceso)
    connection.on('error', (err) => {
      console.error('[NOTIF] Error:', err.message);
    });

  } catch (error) {
    // Si RabbitMQ no está disponible todavía, lo intentamos de nuevo en 5 segundos.
    // Esto es especialmente importante al arrancar en Docker donde el broker
    // puede tardar unos segundos en estar completamente listo.
    console.error('[NOTIF] No se pudo conectar:', error.message);
    console.log('[NOTIF] Reintentando en 5 segundos...');
    setTimeout(conectarRabbitMQ, 5000);
  }
}

// =============================================================================
// ARRANQUE DEL SERVICIO
// =============================================================================
// Mostramos el banner de identificación del servicio antes de conectar.
// Luego iniciamos la conexión a RabbitMQ que mantiene el proceso corriendo
// indefinidamente (el consumer de RabbitMQ no termina solo).
// =============================================================================
console.log('╔══════════════════════════════════════════════╗');
console.log('║     VAR Platform — Notification Backend      ║');
console.log('╚══════════════════════════════════════════════╝\n');

// Iniciamos la conexión a RabbitMQ.
// Esta llamada hace que el proceso Node.js quede "vivo" indefinidamente
// porque el consumer mantiene una conexión TCP abierta con el broker.
conectarRabbitMQ();
