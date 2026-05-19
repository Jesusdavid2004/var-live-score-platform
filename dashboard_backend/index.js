/**
 * dashboard_backend/index.js
 * ==========================
 * T3.2 — Servidor WebSocket + HTTP que:
 *   1. Se suscribe al Topic Exchange "live_updates" (cola dashboard_q)
 *   2. Hace broadcast del marcador JSON a TODOS los navegadores conectados
 *   3. Sirve el dashboard.html estatico desde la misma URL (evita CORS)
 *
 * Puerto: 3003
 * WebSocket endpoint: ws://localhost:3003
 * HTTP (dashboard): http://localhost:3003
 */

const amqp    = require('amqplib');
const express = require('express');
const http    = require('http');
const WebSocket = require('ws');
const path    = require('path');

const RABBITMQ_URL     = process.env.RABBITMQ_URL     || 'amqp://guest:guest@localhost:5672';
const DASHBOARD_PORT   = process.env.DASHBOARD_PORT   || 3003;

// ── App HTTP + WebSocket sobre el mismo servidor ─────────────────────────────
const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

// Sirve dashboard.html desde ../dashboard/
app.use(express.static(path.join(__dirname, '..', 'dashboard')));

// ── Estado en memoria del ultimo marcador conocido ───────────────────────────
// Cuando un navegador nuevo se conecta, le mandamos el estado actual
// para que no vea un dashboard vacio.
let ultimoEstado = null;

// ── Gestión de clientes WebSocket ────────────────────────────────────────────
wss.on('connection', (ws, req) => {
  const clientIp = req.socket.remoteAddress;
  console.log(`[WS] Cliente conectado: ${clientIp} | Total: ${wss.clients.size}`);

  // Si ya tenemos un estado previo, se lo mandamos inmediatamente
  if (ultimoEstado) {
    ws.send(JSON.stringify(ultimoEstado));
    console.log(`[WS] Estado inicial enviado a nuevo cliente`);
  }

  ws.on('close', () => {
    console.log(`[WS] Cliente desconectado | Quedan: ${wss.clients.size}`);
  });

  ws.on('error', (err) => {
    console.error(`[WS] Error en cliente: ${err.message}`);
  });
});

/**
 * broadcast — manda un mensaje a TODOS los clientes conectados.
 * Solo envia a clientes cuya conexion este OPEN (estado 1).
 */
function broadcast(payload) {
  const mensaje = JSON.stringify(payload);
  let enviados = 0;
  wss.clients.forEach((cliente) => {
    if (cliente.readyState === WebSocket.OPEN) {
      cliente.send(mensaje);
      enviados++;
    }
  });
  console.log(`[WS] Broadcast enviado a ${enviados} cliente(s)`);
}

// ── Conexion a RabbitMQ con reconexion automatica ────────────────────────────
async function conectarRabbitMQ() {
  try {
    console.log('[RABBIT] Conectando a RabbitMQ...');
    const connection = await amqp.connect(RABBITMQ_URL);
    const channel    = await connection.createChannel();

    // Prefetch 1: procesamos de a un mensaje para no saturar
    await channel.prefetch(1);

    // Declaramos el exchange y la cola (idempotente — seguro si ya existe)
    await channel.assertExchange('live_updates', 'topic', { durable: true });
    const { queue } = await channel.assertQueue('dashboard_q', { durable: true });
    await channel.bindQueue(queue, 'live_updates', 'score.*');

    console.log('[RABBIT] ✓ Suscrito a live_updates → dashboard_q [score.*]');
    console.log('[RABBIT] Esperando mensajes de marcador...\n');

    // ── Consumer principal ────────────────────────────────────────────────
    channel.consume(queue, (msg) => {
      if (!msg) return;

      try {
        const contenido = msg.content.toString();
        const datos     = JSON.parse(contenido);
        const routingKey = msg.fields.routingKey;

        console.log(`[RABBIT] Mensaje recibido | routing_key: ${routingKey}`);
        console.log(`[RABBIT] Payload: ${contenido}`);

        // Guardamos como ultimo estado conocido
        ultimoEstado = datos;

        // Hacemos broadcast a todos los navegadores
        broadcast(datos);

        // ACK manual: le decimos a RabbitMQ que procesamos el mensaje OK
        channel.ack(msg);

      } catch (parseError) {
        console.error('[RABBIT] Error parseando mensaje:', parseError.message);
        // NACK: rechazamos el mensaje sin re-encolar (evita loop infinito)
        channel.nack(msg, false, false);
      }
    });

    // Manejo de cierre inesperado — reintenta en 5 segundos
    connection.on('close', () => {
      console.warn('[RABBIT] Conexion cerrada. Reintentando en 5s...');
      setTimeout(conectarRabbitMQ, 5000);
    });

    connection.on('error', (err) => {
      console.error('[RABBIT] Error de conexion:', err.message);
    });

  } catch (error) {
    console.error('[RABBIT] No se pudo conectar:', error.message);
    console.log('[RABBIT] Reintentando en 5 segundos...');
    setTimeout(conectarRabbitMQ, 5000);
  }
}

// ── Arranque ─────────────────────────────────────────────────────────────────
server.listen(DASHBOARD_PORT, () => {
  console.log('╔══════════════════════════════════════════════╗');
  console.log('║       VAR Platform — Dashboard Backend       ║');
  console.log('╠══════════════════════════════════════════════╣');
  console.log(`║  HTTP   →  http://localhost:${DASHBOARD_PORT}           ║`);
  console.log(`║  WS     →  ws://localhost:${DASHBOARD_PORT}             ║`);
  console.log('╚══════════════════════════════════════════════╝\n');
  conectarRabbitMQ();
});
