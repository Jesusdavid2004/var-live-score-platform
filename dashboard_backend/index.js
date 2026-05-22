/**
 * dashboard_backend/index.js — Servidor WebSocket + HTTP del dashboard
 * =====================================================================
 * Este archivo es el corazón del sistema de visualización en tiempo real.
 * Hace tres cosas en simultáneo:
 *
 *   1. SERVIDOR HTTP: sirve el archivo dashboard.html al navegador
 *      (evita problemas de CORS porque HTML y WebSocket usan el mismo origen)
 *
 *   2. SERVIDOR WEBSOCKET: mantiene conexiones abiertas con todos los
 *      navegadores y hace broadcast cuando llega un nuevo marcador
 *
 *   3. CONSUMER RABBITMQ: escucha el exchange "live_updates" y reenvía
 *      cada mensaje a todos los navegadores conectados via WebSocket
 *
 * Puerto: 3003
 * Acceso HTTP:      http://localhost:3003
 * Acceso WebSocket: ws://localhost:3003
 */

// amqplib: cliente RabbitMQ para Node.js
const amqp    = require('amqplib');
// express: framework HTTP mínimo para servir el archivo HTML estático
const express = require('express');
// http: módulo nativo de Node.js para crear el servidor TCP
const http    = require('http');
// ws: librería WebSocket para Node.js (más eficiente que socket.io para este caso)
const WebSocket = require('ws');
// path: módulo nativo para construir rutas de archivos multiplataforma
const path    = require('path');

// Leemos la configuración desde variables de entorno (o valores por defecto)
const RABBITMQ_URL   = process.env.RABBITMQ_URL   || 'amqp://guest:guest@localhost:5672';
const DASHBOARD_PORT = process.env.DASHBOARD_PORT || 3003;

// ── Configuración del servidor HTTP + WebSocket ──────────────────────────────
// Express maneja las peticiones HTTP (GET /dashboard.html)
const app = express();
// Parseamos el body JSON de las peticiones HTTP entrantes (necesario para POST /reiniciar)
app.use(express.json());
// Creamos el servidor HTTP sobre Express. Necesitamos el objeto "server"
// explícito porque el WebSocket se monta sobre él (comparten el puerto).
const server = http.createServer(app);
// El servidor WebSocket se monta sobre el mismo servidor HTTP,
// así HTTP y WS comparten el puerto 3003.
const wss = new WebSocket.Server({ server });

// Servimos todos los archivos de la carpeta /dashboard/ como estáticos.
// Cuando el navegador pide http://localhost:3003/dashboard.html,
// Express responde con el archivo dashboard/dashboard.html.
app.use(express.static(path.join(__dirname, '..', 'dashboard')));

// ── Estado en memoria del último marcador conocido ───────────────────────────
// Guardamos el último estado para enviárselo inmediatamente a cualquier
// navegador que se conecte tarde (por ejemplo, después del kickoff).
// Sin esto, el dashboard estaría vacío hasta el próximo evento.
let ultimoEstado = null;

// Canal RabbitMQ compartido para que el endpoint POST /reiniciar pueda publicar
let rabbitChannel = null;

// ── Gestión de clientes WebSocket ────────────────────────────────────────────
// Este evento se dispara cada vez que un navegador abre una conexión WebSocket
wss.on('connection', (ws, req) => {
  const clientIp = req.socket.remoteAddress;
  console.log(`[WS] Cliente conectado: ${clientIp} | Total: ${wss.clients.size}`);

  // Si ya tenemos un estado anterior, se lo enviamos al navegador recién conectado
  // para que vea el marcador actual en lugar de un dashboard vacío
  if (ultimoEstado) {
    ws.send(JSON.stringify(ultimoEstado));
    console.log(`[WS] Estado inicial enviado a nuevo cliente`);
  }

  // Cuando el navegador cierra la pestaña o pierde conexión
  ws.on('close', () => {
    console.log(`[WS] Cliente desconectado | Quedan: ${wss.clients.size}`);
  });

  // Manejo de errores de red en la conexión del cliente
  ws.on('error', (err) => {
    console.error(`[WS] Error en cliente: ${err.message}`);
  });
});

/**
 * broadcast(payload)
 * Envía el objeto payload (como JSON string) a TODOS los navegadores
 * que tienen una conexión WebSocket abierta en este momento.
 *
 * Solo se envía a clientes en estado OPEN (readyState === 1).
 * Los clientes en proceso de conexión o desconexión se saltan
 * para evitar errores de "send on closed connection".
 */
function broadcast(payload) {
  const mensaje = JSON.stringify(payload);
  let enviados = 0;
  wss.clients.forEach((cliente) => {
    // WebSocket.OPEN === 1: la conexión está establecida y lista para enviar
    if (cliente.readyState === WebSocket.OPEN) {
      cliente.send(mensaje);
      enviados++;
    }
  });
  console.log(`[WS] Broadcast enviado a ${enviados} cliente(s)`);
}

// ── Conexión a RabbitMQ con reconexión automática ────────────────────────────
/**
 * conectarRabbitMQ()
 * Establece la conexión con RabbitMQ, declara el exchange y la cola,
 * y arranca el consumer que recibe mensajes de marcador.
 *
 * Si la conexión se cierra inesperadamente (RabbitMQ se reinicia,
 * problema de red, etc.), espera 5 segundos y vuelve a intentar.
 * Esto hace el servicio resiliente a fallos temporales del broker.
 */
async function conectarRabbitMQ() {
  try {
    console.log('[RABBIT] Conectando a RabbitMQ...');
    const connection = await amqp.connect(RABBITMQ_URL);
    const channel    = await connection.createChannel();

    // prefetch(1): procesamos un mensaje a la vez para no saturar el servidor
    // con actualizaciones de marcador antes de que el broadcast termine
    await channel.prefetch(1);

    // Declaramos el exchange (idempotente: no falla si ya existe)
    // El match_state_service publica aquí con routing key "score.<match_id>"
    await channel.assertExchange('live_updates', 'topic', { durable: true });

    // Declaramos la cola y la enlazamos al exchange con el patrón "score.*"
    // El * coincide con cualquier partido (score.123, score.456, etc.)
    const { queue } = await channel.assertQueue('dashboard_q', { durable: true });
    await channel.bindQueue(queue, 'live_updates', 'score.*');

    // Guardamos el canal en la variable compartida para que POST /reiniciar lo use
    rabbitChannel = channel;

    console.log('[RABBIT] ✓ Suscrito a live_updates → dashboard_q [score.*]');
    console.log('[RABBIT] Esperando mensajes de marcador...\n');

    // ── Consumer: procesa cada mensaje de marcador ───────────────────────────
    channel.consume(queue, (msg) => {
      if (!msg) return;

      try {
        const contenido = msg.content.toString();
        const datos     = JSON.parse(contenido); // Parseamos el JSON del marcador
        const routingKey = msg.fields.routingKey;

        console.log(`[RABBIT] Mensaje recibido | routing_key: ${routingKey}`);
        console.log(`[RABBIT] Payload: ${contenido}`);

        // Actualizamos el último estado conocido para los clientes que conecten después
        ultimoEstado = datos;

        // Enviamos el marcador actualizado a TODOS los navegadores conectados
        broadcast(datos);

        // ACK manual: le decimos a RabbitMQ que el mensaje fue procesado exitosamente.
        // RabbitMQ lo elimina de la cola solo después de recibir este ACK.
        channel.ack(msg);

      } catch (parseError) {
        console.error('[RABBIT] Error parseando mensaje:', parseError.message);
        // NACK sin re-encolar (false, false): rechazamos el mensaje malformado
        // para que no vuelva a la cola y cause un loop infinito de reintentos
        channel.nack(msg, false, false);
      }
    });

    // Si la conexión se cierra (RabbitMQ reiniciado, timeout de red, etc.),
    // reintentamos la conexión completa después de 5 segundos
    connection.on('close', () => {
      console.warn('[RABBIT] Conexion cerrada. Reintentando en 5s...');
      setTimeout(conectarRabbitMQ, 5000);
    });

    connection.on('error', (err) => {
      console.error('[RABBIT] Error de conexion:', err.message);
    });

  } catch (error) {
    // Si no podemos conectar (RabbitMQ no está listo todavía), reintentamos
    console.error('[RABBIT] No se pudo conectar:', error.message);
    console.log('[RABBIT] Reintentando en 5 segundos...');
    setTimeout(conectarRabbitMQ, 5000);
  }
}

// ── Endpoint POST /reiniciar ─────────────────────────────────────────────────
/**
 * POST /reiniciar
 *   1. Resetea ultimoEstado a null
 *   2. Broadcast KICKOFF 0-0 a todos los clientes WebSocket
 *   3. Reinicia el contenedor live_feed_producer via docker restart
 */
app.post('/reiniciar', (req, res) => {
  console.log('[HTTP] POST /reiniciar recibido');

  // 1. Resetear el estado en memoria
  ultimoEstado = null;

  // 2. Broadcast del KICKOFF 0-0 a todos los clientes WebSocket
  broadcast({
    event_type: 'KICKOFF',
    home: 0,
    away: 0,
    match_id: '123',
    timestamp: new Date().toISOString(),
  });

  // 3. Reiniciar el contenedor live_feed_producer
  require('child_process').exec('docker restart live_feed_producer', (err, stdout) => {
    if (err) {
      console.error('[HTTP] Error reiniciando live_feed_producer:', err.message);
    } else {
      console.log('[HTTP] live_feed_producer reiniciado:', stdout.trim());
    }
  });

  res.json({ ok: true });
});

// ── Arranque del servidor ────────────────────────────────────────────────────
// Iniciamos el servidor HTTP+WebSocket en el puerto configurado.
// Solo después de que el servidor esté escuchando, iniciamos la
// conexión a RabbitMQ para evitar perder mensajes durante el arranque.
server.listen(DASHBOARD_PORT, () => {
  console.log('╔══════════════════════════════════════════════╗');
  console.log('║       VAR Platform — Dashboard Backend       ║');
  console.log('╠══════════════════════════════════════════════╣');
  console.log(`║  HTTP   →  http://localhost:${DASHBOARD_PORT}           ║`);
  console.log(`║  WS     →  ws://localhost:${DASHBOARD_PORT}             ║`);
  console.log('╚══════════════════════════════════════════════╝\n');
  // Iniciamos la conexión a RabbitMQ una vez que el servidor HTTP está listo
  conectarRabbitMQ();
});
