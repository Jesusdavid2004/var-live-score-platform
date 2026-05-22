// =============================================================================
// dashboard_backend/index.js — Servidor WebSocket + HTTP del dashboard
// =============================================================================
// Este archivo es el corazón del sistema de visualización en tiempo real.
// Actúa como PUENTE entre RabbitMQ (donde llegan los eventos del partido)
// y los navegadores de los usuarios (que muestran el marcador).
//
// ¿Qué hace exactamente este archivo?
//   1. SERVIDOR HTTP: sirve el archivo dashboard.html al navegador.
//      Usar el mismo servidor para HTTP y WebSocket evita problemas de CORS
//      porque el HTML y el WebSocket tienen el mismo origen (mismo puerto).
//
//   2. SERVIDOR WEBSOCKET: mantiene conexiones abiertas con TODOS los
//      navegadores que tienen el dashboard abierto. Cuando llega un nuevo
//      evento de partido, se lo envía a todos simultáneamente (broadcast).
//
//   3. CONSUMER RABBITMQ: se suscribe al exchange "live_updates" y recibe
//      cada actualización de marcador que publica el match_state_service.
//
//   4. ENDPOINT POST /reiniciar: recibe la señal del botón del dashboard,
//      resetea el estado del servidor y publica un mensaje RESTART a RabbitMQ
//      para que el live_feed_producer comience un nuevo partido.
//
// Posición en la arquitectura:
//   match_state_service → RabbitMQ (live_updates) → ESTE ARCHIVO → WebSocket → navegador
//   Dashboard (botón)   → HTTP POST /reiniciar    → ESTE ARCHIVO → RabbitMQ (restart_commands) → live_feed_producer
//
// Puerto: 3003
// Acceso HTTP:      http://localhost:3003
// Acceso WebSocket: ws://localhost:3003
// =============================================================================

// amqplib: cliente oficial de RabbitMQ para Node.js
// Permite conectarse al broker AMQP y consumir/publicar mensajes
const amqp    = require('amqplib');

// express: framework HTTP minimalista para Node.js
// Lo usamos únicamente para servir el archivo dashboard.html estático
// y para definir el endpoint POST /reiniciar
const express = require('express');

// http: módulo nativo de Node.js para crear un servidor TCP
// Necesitamos este objeto explícito porque el servidor WebSocket (ws)
// se monta SOBRE él, no sobre Express directamente
const http    = require('http');

// ws: librería WebSocket para Node.js
// Más liviana que socket.io y suficiente para nuestro caso de uso:
// solo necesitamos broadcast unidireccional (servidor → clientes)
const WebSocket = require('ws');

// path: módulo nativo de Node.js para construir rutas de archivos
// Necesario para construir rutas multiplataforma (Windows usa \ en lugar de /)
const path    = require('path');

// ── Configuración desde variables de entorno ──────────────────────────────────
// Leemos los parámetros de conexión desde .env para que el mismo código
// funcione en local (localhost) y en Docker (rabbitmq como hostname)
const RABBITMQ_URL   = process.env.RABBITMQ_URL   || 'amqp://guest:guest@localhost:5672';
const DASHBOARD_PORT = process.env.DASHBOARD_PORT || 3003;

// ── Creación del servidor HTTP + Express ──────────────────────────────────────
// Express maneja las peticiones HTTP normales (GET para el HTML, POST /reiniciar)
const app = express();

// Middleware que permite leer el body de peticiones POST con formato JSON.
// Sin esto, req.body estaría undefined en el endpoint POST /reiniciar.
app.use(express.json());

// Creamos el servidor HTTP NATIVO sobre Express.
// Necesitamos este objeto porque el servidor WebSocket (wss) se monta sobre él.
// Si usáramos app.listen() directamente, no podríamos compartir el puerto.
const server = http.createServer(app);

// Montamos el servidor WebSocket sobre el mismo servidor HTTP.
// Esto permite que HTTP (puerto 3003) y WebSocket (ws://...:3003) compartan
// el mismo puerto, simplificando la configuración de red y Docker.
const wss = new WebSocket.Server({ server });

// Configuramos Express para servir estáticamente todos los archivos de /dashboard/.
// Cuando el navegador pide http://localhost:3003/dashboard.html,
// Express busca el archivo en ../dashboard/dashboard.html y lo sirve.
// path.join con __dirname construye la ruta absoluta correcta en cualquier SO.
app.use(express.static(path.join(__dirname, '..', 'dashboard')));

// ── Estado en memoria: último marcador conocido ───────────────────────────────
// Guardamos el último estado recibido de RabbitMQ para enviárselo a los
// navegadores que se conecten DESPUÉS del inicio del partido.
// Sin esto, un usuario que abre el dashboard a mitad del partido vería
// el marcador en blanco hasta que llegue el próximo evento.
let ultimoEstado = null;

// ── Canal RabbitMQ compartido ─────────────────────────────────────────────────
// Guardamos el canal activo de RabbitMQ en una variable de módulo para que
// el endpoint POST /reiniciar pueda usarlo para publicar mensajes.
// Se asigna dentro de conectarRabbitMQ() cuando la conexión se establece.
let rabbitChannel = null;

// =============================================================================
// GESTIÓN DE CLIENTES WEBSOCKET
// =============================================================================
// Cada vez que un navegador abre el dashboard, se establece una nueva
// conexión WebSocket y se dispara este evento 'connection'.
// =============================================================================
wss.on('connection', (ws, req) => {
  // Registramos la IP del cliente para debugging y monitoreo
  const clientIp = req.socket.remoteAddress;
  console.log(`[WS] Cliente conectado: ${clientIp} | Total: ${wss.clients.size}`);

  // Si ya tenemos un estado del partido (ya ocurrió algún evento),
  // se lo enviamos INMEDIATAMENTE al nuevo cliente para que vea el
  // marcador actual en lugar de una pantalla en blanco.
  // Esto es el patrón "catch-up": el nuevo cliente se pone al día solo.
  if (ultimoEstado) {
    ws.send(JSON.stringify(ultimoEstado));
    console.log(`[WS] Estado inicial enviado a nuevo cliente`);
  }

  // Registramos cuando este cliente se desconecta (cierra la pestaña, etc.)
  ws.on('close', () => {
    console.log(`[WS] Cliente desconectado | Quedan: ${wss.clients.size}`);
  });

  // Manejamos errores de red en la conexión individual del cliente
  ws.on('error', (err) => {
    console.error(`[WS] Error en cliente: ${err.message}`);
  });
});

// =============================================================================
// FUNCIÓN: broadcast(payload)
// =============================================================================
// Envía un objeto JSON a TODOS los navegadores conectados simultáneamente.
// Es el núcleo del sistema de tiempo real: cuando llega un evento de partido,
// se propaga instantáneamente a todos los usuarios que tienen el dashboard abierto.
//
// ¿Por qué verificar readyState === WebSocket.OPEN?
// Un cliente puede estar en proceso de conectarse (CONNECTING) o desconectándose
// (CLOSING/CLOSED). Solo enviamos a los que están completamente conectados (OPEN)
// para evitar errores de "send on closed connection" que crashearían el servidor.
// =============================================================================
function broadcast(payload) {
  const mensaje = JSON.stringify(payload); // Serializamos el objeto a JSON string
  let enviados = 0;

  // Iteramos sobre TODOS los clientes conectados al servidor WebSocket
  wss.clients.forEach((cliente) => {
    // WebSocket.OPEN === 1: la conexión está activa y lista para recibir mensajes
    if (cliente.readyState === WebSocket.OPEN) {
      cliente.send(mensaje); // Enviamos el JSON al navegador del cliente
      enviados++;
    }
  });
  console.log(`[WS] Broadcast enviado a ${enviados} cliente(s)`);
}

// =============================================================================
// FUNCIÓN ASYNC: conectarRabbitMQ()
// =============================================================================
// Establece la conexión con RabbitMQ, declara el exchange y la cola,
// y arranca el consumer que recibe los mensajes de marcador del partido.
//
// ¿Por qué tiene reconexión automática?
// En Docker, los contenedores no siempre arrancan en el orden correcto.
// RabbitMQ puede tardar unos segundos en estar listo. La reconexión automática
// permite que este servicio se recupere solo sin intervención manual.
//
// Flujo de mensajes que maneja esta función:
//   match_state_service → RabbitMQ exchange "live_updates" → cola "dashboard_q"
//   → esta función → broadcast() → todos los navegadores conectados
// =============================================================================
async function conectarRabbitMQ() {
  try {
    console.log('[RABBIT] Conectando a RabbitMQ...');

    // Establecemos la conexión TCP con el broker de RabbitMQ
    const connection = await amqp.connect(RABBITMQ_URL);

    // Creamos un canal de comunicación sobre la conexión.
    // Un canal es más eficiente que una conexión completa para cada operación.
    const channel    = await connection.createChannel();

    // prefetch(1): procesamos un mensaje a la vez.
    // Sin esto, RabbitMQ podría enviar todos los mensajes pendientes a la vez,
    // lo que podría saturar el broadcast antes de que los clientes los procesen.
    await channel.prefetch(1);

    // Declaramos el Topic Exchange "live_updates" (idempotente: no falla si ya existe).
    // El match_state_service publica aquí con routing key "score.match_<id>".
    // durable: true → el exchange sobrevive reinicios de RabbitMQ.
    await channel.assertExchange('live_updates', 'topic', { durable: true });

    // Declaramos la cola del dashboard (durable: los mensajes no se pierden)
    const { queue } = await channel.assertQueue('dashboard_q', { durable: true });

    // Enlazamos la cola al exchange con el patrón "score.*".
    // El patrón * en Topic Exchange coincide con EXACTAMENTE UNA palabra.
    // "score.*" recibirá "score.match_123", "score.match_456", etc.
    await channel.bindQueue(queue, 'live_updates', 'score.*');

    // Guardamos el canal en la variable de módulo para que POST /reiniciar lo use
    rabbitChannel = channel;

    console.log('[RABBIT] ✓ Suscrito a live_updates → dashboard_q [score.*]');
    console.log('[RABBIT] Esperando mensajes de marcador...\n');

    // ── Consumer: procesa cada mensaje de marcador ────────────────────────────
    // channel.consume() registra una función que se ejecuta por cada mensaje nuevo.
    // Es el equivalente asíncrono de un bucle while(true) { leerMensaje() }.
    channel.consume(queue, (msg) => {
      if (!msg) return; // msg es null si el consumer fue cancelado por el broker

      try {
        const contenido  = msg.content.toString();          // Buffer → string
        const datos      = JSON.parse(contenido);           // string → objeto JS
        const routingKey = msg.fields.routingKey;           // Clave con la que se publicó

        console.log(`[RABBIT] Mensaje recibido | routing_key: ${routingKey}`);
        console.log(`[RABBIT] Payload: ${contenido}`);

        // Guardamos el estado actual para enviárselo a los clientes que conecten luego
        ultimoEstado = datos;

        // Enviamos el marcador actualizado a TODOS los navegadores conectados
        broadcast(datos);

        // ACK manual: confirmamos a RabbitMQ que procesamos el mensaje correctamente.
        // Solo después de este ACK, RabbitMQ elimina el mensaje de la cola.
        // Si el proceso muere antes del ACK, RabbitMQ re-entrega el mensaje.
        channel.ack(msg);

      } catch (parseError) {
        console.error('[RABBIT] Error parseando mensaje:', parseError.message);
        // NACK sin re-encolar (false, false): rechazamos el mensaje malformado
        // para que no vuelva a la cola y cause un loop infinito de errores
        channel.nack(msg, false, false);
      }
    });

    // Si la conexión se cierra inesperadamente (RabbitMQ reiniciado, timeout, etc.),
    // esperamos 5 segundos y volvemos a intentar todo el proceso de conexión.
    connection.on('close', () => {
      console.warn('[RABBIT] Conexion cerrada. Reintentando en 5s...');
      rabbitChannel = null; // Reseteamos el canal para evitar usarlo cuando ya no existe
      setTimeout(conectarRabbitMQ, 5000);
    });

    connection.on('error', (err) => {
      console.error('[RABBIT] Error de conexion:', err.message);
    });

  } catch (error) {
    // Si RabbitMQ no está disponible todavía (ej: Docker aún iniciando),
    // reintentamos en 5 segundos en lugar de crashear el proceso
    console.error('[RABBIT] No se pudo conectar:', error.message);
    console.log('[RABBIT] Reintentando en 5 segundos...');
    setTimeout(conectarRabbitMQ, 5000);
  }
}

// =============================================================================
// ENDPOINT HTTP: POST /reiniciar
// =============================================================================
// Este endpoint es llamado por el botón "Reiniciar Partido" del dashboard.
// Coordina tres acciones para reiniciar el partido de extremo a extremo:
//
//   1. Resetea ultimoEstado a null: así los nuevos clientes que conecten
//      no recibirán el estado del partido anterior.
//
//   2. Broadcast KICKOFF 0-0 vía WebSocket: todos los navegadores reciben
//      inmediatamente el reinicio sin esperar que el producer publique.
//
//   3. Publica { command: "RESTART" } a la cola "restart_commands":
//      el live_feed_producer escucha esta cola y al recibir el mensaje
//      vuelve a ejecutar runScenario() desde KICKOFF.
//
// ¿Por qué no reiniciar el contenedor Docker directamente?
// El contenedor puede no tener acceso al socket de Docker. La señal
// por RabbitMQ es más portable y desacoplada: no depende de privilegios
// de Docker y funciona en cualquier entorno (local, cloud, etc.).
// =============================================================================
app.post('/reiniciar', (req, res) => {
  console.log('[HTTP] POST /reiniciar recibido');

  // Paso 1: Resetear el último estado conocido a null.
  // Si un nuevo cliente conecta ahora, recibirá undefined (marcador limpio)
  // en lugar del estado del partido anterior.
  ultimoEstado = null;

  // Paso 2: Broadcast del KICKOFF 0-0 a todos los clientes WebSocket activos.
  // Esto da feedback visual INMEDIATO al usuario que presionó el botón
  // mientras el live_feed_producer arranca el nuevo partido.
  broadcast({
    event_type: 'KICKOFF', // Tipo de evento para que el dashboard lo procese correctamente
    home: 0,               // Marcador reiniciado a cero
    away: 0,               // Marcador reiniciado a cero
    match_id: '123',       // ID del partido (fijo en esta simulación)
    timestamp: new Date().toISOString(), // Timestamp del reinicio
  });

  // Paso 3: Publicar la señal RESTART a RabbitMQ para que el live_feed_producer
  // comience a correr el escenario del partido desde el inicio.
  if (rabbitChannel) {
    try {
      // Nos aseguramos de que la cola "restart_commands" exista antes de publicar
      // assertQueue es idempotente: no falla si ya existe
      rabbitChannel.assertQueue('restart_commands', { durable: true });

      // sendToQueue publica directamente a la cola (sin pasar por exchange).
      // persistent: true → el mensaje se guarda en disco, no se pierde si
      // RabbitMQ se reinicia antes de que el producer lo consuma.
      rabbitChannel.sendToQueue(
        'restart_commands',
        Buffer.from(JSON.stringify({ command: 'RESTART' })), // Convertimos el objeto a Buffer
        { persistent: true }
      );
      console.log('[HTTP] Comando RESTART publicado a restart_commands');
    } catch (err) {
      console.error('[HTTP] Error publicando RESTART:', err.message);
    }
  } else {
    // Esto puede ocurrir si RabbitMQ aún no se ha conectado al arrancar
    console.warn('[HTTP] rabbitChannel no disponible aún');
  }

  // Respondemos con { ok: true } para que el dashboard actualice el indicador
  // a verde "En vivo" (ver función reiniciarPartido() en dashboard.html)
  res.json({ ok: true });
});

// =============================================================================
// ARRANQUE DEL SERVIDOR
// =============================================================================
// Iniciamos el servidor HTTP+WebSocket en el puerto configurado.
// Usamos server.listen() (no app.listen()) porque el objeto 'server' es el
// que tiene montado el servidor WebSocket (wss).
// Solo DESPUÉS de que el servidor esté escuchando iniciamos la conexión a
// RabbitMQ para evitar perder mensajes durante el arranque.
// =============================================================================
server.listen(DASHBOARD_PORT, () => {
  // Banner visual de confirmación de que el servidor está listo
  console.log('╔══════════════════════════════════════════════╗');
  console.log('║       VAR Platform — Dashboard Backend       ║');
  console.log('╠══════════════════════════════════════════════╣');
  console.log(`║  HTTP   →  http://localhost:${DASHBOARD_PORT}           ║`);
  console.log(`║  WS     →  ws://localhost:${DASHBOARD_PORT}             ║`);
  console.log('╚══════════════════════════════════════════════╝\n');

  // Una vez que el servidor HTTP+WS está listo, conectamos a RabbitMQ
  // para empezar a recibir actualizaciones de marcador
  conectarRabbitMQ();
});
