# Persona 3 — RabbitMQ & Capa de Presentación
> VAR Live Score Platform · Sistemas Distribuidos

Esta carpeta contiene **toda la capa de mensajería RabbitMQ y el frontend** del sistema.
Aquí vive el dashboard que muestra el marcador en tiempo real y el flujo VAR completo.

---

## Estructura del proyecto

```
persona3-rabbitmq-ui/
├── dashboard_backend/
│   ├── index.js              # Servidor WebSocket + HTTP (T3.2)
│   └── setup-exchanges.js    # Declaración de exchanges RabbitMQ (T3.1)
├── notification_backend/
│   └── index.js              # Consumer de alertas por consola (T3.3)
├── dashboard/
│   └── dashboard.html        # Frontend marcador tiempo real (T3.4)
├── Dockerfile.dashboard
├── Dockerfile.notification
├── docker-compose.persona3.yml   # Fragmento para que P1 integre
├── package.json
└── .env.example
```

---

## Topología RabbitMQ — LO QUE P2 NECESITA SABER

> **P2 (Arévalo):** estos son los exchanges que ya están declarados.
> Solo publicas en ellos, no tienes que crearlos tú.

| Canal | Tipo | Cómo publicar desde P2 |
|-------|------|------------------------|
| `live_updates` | Topic Exchange | routing key: `score.<match_id>` (ej: `score.123`) |
| `live_alerts` | Fanout Exchange | sin routing key — publica directo al exchange |
| `betting_commands` | Work Queue | publica directo a la cola con el mismo nombre |

### Formato exacto de los mensajes que espera P3

**→ live_updates** (routing key: `score.123`)
```json
{
  "match_id":   "123",
  "event_type": "GOAL",
  "event_id":   "evt_g001",
  "team":       "home",
  "home":        1,
  "away":        0
}
```

**→ live_alerts** (fanout, sin routing key)
```json
{
  "message":  "¡GOL del equipo local!",
  "severity": "info",
  "match_id": "123"
}
```
> `severity` puede ser `"info"` (goles normales) o `"warning"` (VAR, gol anulado)

**→ betting_commands** (work queue)
```json
{
  "command":  "SUSPEND_BETS",
  "match_id": "123"
}
```

---

## Cómo correr en local (sin Docker, para desarrollo)

### 1. Instalar dependencias
```bash
npm install
```

### 2. Levantar RabbitMQ con Docker
```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```
> Espera unos 10-15 segundos antes del siguiente paso — RabbitMQ tarda en arrancar.

### 3. Declarar los exchanges (solo una vez)
```bash
node dashboard_backend/setup-exchanges.js
```
Deberías ver:
```
[SETUP] ✓ Exchange "live_updates" (topic) declarado
[SETUP] ✓ Cola "dashboard_q" enlazada con binding "score.*"
[SETUP] ✓ Exchange "live_alerts" (fanout) declarado
[SETUP] ✓ Cola "alerts_q" enlazada al fanout
[SETUP] ✓ Work Queue "betting_commands" declarada
```

### 4. Correr los servicios (dos terminales)
```bash
# Terminal 1
node dashboard_backend/index.js

# Terminal 2
node notification_backend/index.js
```

### 5. Abrir el dashboard en el navegador
```
http://localhost:3003/dashboard.html
```
Deberías ver el marcador en `-:-` y el punto verde **"En vivo"** arriba a la derecha.

### 6. Verificar la topología en el panel RabbitMQ
```
http://localhost:15672
Usuario: guest   |   Password: guest
```
En la pestaña **Exchanges** deben aparecer `live_updates` y `live_alerts`.
En **Queues** deben aparecer `dashboard_q`, `alerts_q` y `betting_commands`.

---

## Prueba manual del flujo VAR (para verificar antes del Sync 3)

Abre una terminal extra y corre estos comandos **uno por uno**, esperando ~3 segundos entre cada uno. Mira el dashboard mientras los ejecutas.

```bash
# 1. KICKOFF
node -e "const amqp = require('amqplib'); (async () => { const conn = await amqp.connect('amqp://localhost:5672'); const ch = await conn.createChannel(); ch.publish('live_updates', 'score.123', Buffer.from(JSON.stringify({ match_id: '123', event_type: 'KICKOFF', home: 0, away: 0 }))); console.log('KICKOFF enviado'); setTimeout(() => conn.close(), 500); })();"

# 2. GOL del local → marcador pasa a 1-0
node -e "const amqp = require('amqplib'); (async () => { const conn = await amqp.connect('amqp://localhost:5672'); const ch = await conn.createChannel(); ch.publish('live_updates', 'score.123', Buffer.from(JSON.stringify({ match_id: '123', event_type: 'GOAL', event_id: 'evt_g001', team: 'home', home: 1, away: 0 }))); console.log('GOAL enviado'); setTimeout(() => conn.close(), 500); })();"

# 3. VAR CHECK → banner naranja parpadeando en el dashboard
node -e "const amqp = require('amqplib'); (async () => { const conn = await amqp.connect('amqp://localhost:5672'); const ch = await conn.createChannel(); ch.publish('live_updates', 'score.123', Buffer.from(JSON.stringify({ match_id: '123', event_type: 'VAR_CHECK', home: 1, away: 0 }))); console.log('VAR_CHECK enviado'); setTimeout(() => conn.close(), 500); })();"

# 4. GOL ANULADO → marcador vuelve a 0-0 con animación roja
node -e "const amqp = require('amqplib'); (async () => { const conn = await amqp.connect('amqp://localhost:5672'); const ch = await conn.createChannel(); ch.publish('live_updates', 'score.123', Buffer.from(JSON.stringify({ match_id: '123', event_type: 'GOAL_ANNULLED', home: 0, away: 0 }))); console.log('GOAL_ANNULLED enviado'); setTimeout(() => conn.close(), 500); })();"
```

**Resultado esperado en el dashboard:**
```
🏁 Partido iniciado
⚽ GOL del equipo local → 1 : 0
📺 Banner naranja "VAR EN REVISIÓN" parpadeando
❌ Gol ANULADO → 0 : 0  (animación roja)
```
Eso confirma que el flujo VAR completo funciona de extremo a extremo.

---

## Para Docker (integración con P1 — Jesús)

> **P1 (Jesús):** copia los servicios de `docker-compose.persona3.yml` al compose principal.
> El único requisito es que el servicio `rabbitmq` tenga healthcheck y esté en la red `var_network`.

El fragmento mínimo que necesita el compose principal para que P3 funcione:

```yaml
rabbitmq:
  image: rabbitmq:3-management
  ports:
    - "5672:5672"
    - "15672:15672"
  healthcheck:
    test: ["CMD", "rabbitmq-diagnostics", "ping"]
    interval: 10s
    timeout: 5s
    retries: 5
  networks:
    - var_network

networks:
  var_network:
    driver: bridge
```

Los servicios `dashboard_backend` y `notification_backend` ya tienen `depends_on: rabbitmq: condition: service_healthy`, así que esperan automáticamente a que RabbitMQ esté listo antes de arrancar.

---

## Checklist de entrega (T3)

- [x] T3.1 Exchanges RabbitMQ declarados y documentados
- [x] T3.2 `dashboard_backend` con WebSocket activo (puerto 3003)
- [x] T3.3 `notification_backend` imprimiendo alertas en consola
- [x] T3.4 `dashboard.html` con marcador en tiempo real y estado VAR
- [x] T3.5 Panel RabbitMQ verificado en `:15672`
- [x] T3.6 Prueba manual VAR completa: `1-0 → VAR → 0-0` ✔