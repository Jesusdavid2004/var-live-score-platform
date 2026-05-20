# VAR Live Score Platform

Sistema distribuido de marcador en tiempo real con flujo VAR, desarrollado como trabajo final de Sistemas Distribuidos. Simula la infraestructura de mensajería que usan plataformas de deportes en vivo para procesar eventos de partido, actualizar marcadores y gestionar apuestas de manera concurrente.

---

## Integrantes del equipo

| Nombre | Rol en el proyecto |
|--------|-------------------|
| Jesus Villota | Infraestructura Kafka, pipeline de eventos, integración |
| Santiago Arevalo | Lógica de servicios, match state, betting worker |
| Juan Felipe Mora Revelo| Capa RabbitMQ, dashboard WebSocket, notificaciones |

---

## Arquitectura del sistema

El sistema está compuesto por **9 servicios Docker** que se comunican mediante dos brokers de mensajería: **Apache Kafka** (para el flujo de eventos del partido) y **RabbitMQ** (para la distribución a clientes finales).

```
┌─────────────────────────────────────────────────────────────────┐
│                        BROKERS                                   │
│                                                                  │
│   ┌──────────────┐              ┌──────────────────────────┐    │
│   │    Kafka     │              │        RabbitMQ          │    │
│   │  (Zookeeper) │              │  live_updates  (topic)   │    │
│   │              │              │  live_alerts   (fanout)  │    │
│   │  match_events│              │  betting_commands (queue)│    │
│   └──────────────┘              └──────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

### Diagrama de flujo completo

```
                        ┌──────────────────┐
                        │ live_feed_producer│
                        │  (simula partido) │
                        └────────┬─────────┘
                                 │ publica eventos JSON
                                 ▼
                    ┌────────────────────────┐
                    │   Kafka: match_events   │
                    │   (3 particiones)       │
                    └────────────────────────┘
                      │           │          │
           ┌──────────┘   ┌───────┘   ┌─────┘
           ▼               ▼           ▼
  ┌──────────────┐ ┌──────────────┐ ┌──────────────────────┐
  │match_state   │ │  betting_    │ │  historical_archiver  │
  │_service      │ │  suspension  │ │  (registra todos los  │
  │(marcador)    │ │  _service    │ │   eventos en consola) │
  └──────┬───────┘ └──────┬───────┘ └──────────────────────┘
         │                │
         │ score update   │ SUSPEND/RESUME
         │ + alert        │ _BETS command
         ▼                ▼
  ┌──────────────┐ ┌──────────────┐
  │ live_updates │ │betting_      │
  │ (RabbitMQ)   │ │commands      │
  │ topic exch.  │ │(RabbitMQ     │
  └──────┬───────┘ │ work queue)  │
         │         └──────┬───────┘
         │                │
         ▼                ▼
  ┌──────────────┐ ┌──────────────┐
  │dashboard_    │ │betting_      │
  │backend       │ │worker        │
  │(WebSocket)   │ │(procesa cmd) │
  └──────┬───────┘ └──────────────┘
         │
         │ WebSocket broadcast
         ▼
  ┌──────────────┐
  │  dashboard   │
  │  .html       │
  │  (navegador) │
  └──────────────┘

  RabbitMQ: live_alerts (fanout)
         │
         ▼
  ┌──────────────────┐
  │notification_     │
  │backend           │
  │(alertas consola) │
  └──────────────────┘
```

### Descripción de cada componente

| Servicio | Tecnología | Rol |
|----------|-----------|-----|
| **zookeeper** | Apache ZooKeeper 7.5 | Coordinador de clúster para Kafka |
| **kafka** | Apache Kafka 7.5 | Broker de eventos del partido |
| **kafka-init** | cp-kafka (script) | Crea el topic `match_events` al arrancar |
| **rabbitmq** | RabbitMQ 3 Management | Broker de mensajería para distribución final |
| **live_feed_producer** | Node.js + KafkaJS | Simula el partido publicando eventos a Kafka |
| **match_state_service** | Node.js + KafkaJS + amqplib | Mantiene el marcador y publica a RabbitMQ |
| **betting_suspension_service** | Node.js + KafkaJS + amqplib | Suspende/reanuda apuestas según los eventos |
| **historical_archiver** | Node.js + KafkaJS | Registra todos los eventos para auditoría |
| **betting_worker** | Node.js + amqplib | Procesa comandos de apuestas de la Work Queue |
| **dashboard_backend** | Node.js + Express + ws | Servidor WebSocket que actualiza el marcador en vivo |
| **notification_backend** | Node.js + amqplib | Consumer de alertas (goles, VAR) |
| **dashboard** | HTML + CSS + JS vanilla | Interfaz visual del marcador en tiempo real |

---

## Tecnologías utilizadas

| Tecnología | Por qué se eligió |
|-----------|-------------------|
| **Apache Kafka** | Permite múltiples consumidores independientes del mismo stream de eventos. Cada servicio lee a su propio ritmo sin interferir con los demás. Garantiza orden de mensajes por partición. |
| **RabbitMQ** | Ideal para el último tramo de distribución: soporta exchanges topic/fanout para enrutar mensajes, work queues para balanceo de carga entre workers, y ACK manual para garantizar procesamiento. |
| **Node.js** | Modelo de I/O no bloqueante ideal para servicios que pasan la mayor parte del tiempo esperando mensajes de brokers. Permite escribir todos los microservicios en el mismo lenguaje. |
| **KafkaJS** | Cliente Kafka moderno para Node.js con soporte nativo de async/await y grupos de consumidores. |
| **amqplib** | Cliente AMQP 0-9-1 para Node.js, protocolo nativo de RabbitMQ. |
| **Docker Compose** | Orquesta todos los servicios con una sola red virtual, healthchecks y dependencias correctas entre contenedores. |
| **WebSocket (ws)** | Protocolo bidireccional sobre TCP para enviar actualizaciones del marcador al navegador sin que este tenga que hacer polling. |

---

## Requisitos previos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado y corriendo
- [Git](https://git-scm.com/) para clonar el repositorio
- No se necesita Node.js ni nada más en la máquina host

---

## Cómo correr el proyecto

### 1. Clonar el repositorio

```bash
git clone <url-del-repositorio>
cd var-live-score-platform
```

### 2. Configurar las variables de entorno

```bash
cp .env.example .env
```

El archivo `.env` por defecto funciona sin modificaciones. Las variables más importantes son:

```env
KAFKA_BROKER=kafka:9092
KAFKA_TOPIC=match_events
RABBITMQ_URL=amqp://guest:guest@rabbitmq:5672
VAR_DELAY_MS=20000
MATCH_ID=123
```

### 3. Levantar todos los servicios

```bash
docker-compose up --build
```

Esto levanta en orden:
1. Zookeeper → Kafka (espera healthcheck)
2. kafka-init (crea el topic `match_events`)
3. RabbitMQ (espera healthcheck)
4. Todos los microservicios en paralelo

### 4. Abrir el dashboard en el navegador

```
http://localhost:3003/dashboard.html
```

Verás el marcador `0 : 0` y el indicador **En vivo** en verde. El partido comienza automáticamente cuando el `live_feed_producer` arranca.

### 5. Ver los paneles de administración

| Panel | URL | Credenciales |
|-------|-----|-------------|
| RabbitMQ Management | http://localhost:15672 | guest / guest |
| Zookeeper | localhost:2181 | — |
| Kafka | localhost:9092 (TCP) | — |

### 6. Detener todo

```bash
# Solo detener (preserva volúmenes)
docker-compose down

# Detener y limpiar todos los datos
docker-compose down -v
```

---

## El flujo VAR paso a paso

El sistema simula un partido completo con el siguiente guión:

```
Tiempo  Evento           Marcador  Apuestas   Alertas
──────  ───────────────  ────────  ─────────  ───────────────────────
  0s    KICKOFF          0 - 0     REANUDAN   —
  3s    GOAL (home)      1 - 0     SUSPENDEN  ⚽ GOL del equipo local
  5s    VAR_CHECK        1 - 0     SUSPENDEN  📺 VAR en revisión...
 25s    GOAL_ANNULLED    0 - 0     REANUDAN   🚨 GOL ANULADO POR EL VAR
 28s    FOUL (away)      0 - 0     —          —
 31s    MATCH_END        0 - 0     REANUDAN   —
```

**Lo que sucede internamente en cada paso:**

1. **KICKOFF**: `live_feed_producer` publica en Kafka → los 3 consumidores reciben el evento simultáneamente → `match_state_service` inicializa el marcador en 0-0 y lo publica a RabbitMQ → `dashboard_backend` hace WebSocket broadcast → el marcador aparece en el navegador.

2. **GOAL**: `match_state_service` suma 1 al marcador (1-0) → publica alerta en `live_alerts` → `betting_suspension_service` detecta GOAL y envía `SUSPEND_BETS` a la Work Queue → `betting_worker` recibe y registra la suspensión.

3. **VAR_CHECK**: El marcador sigue en 1-0 pero el dashboard muestra el banner de revisión VAR → las apuestas permanecen suspendidas. El sistema espera `VAR_DELAY_MS` milisegundos (20 segundos por defecto).

4. **GOAL_ANNULLED**: `match_state_service` busca el gol original en su registro por `event_id` y resta 1 → marcador vuelve a 0-0 → publica alerta `warning` → `betting_suspension_service` envía `RESUME_BETS` → las apuestas se reanudan.

5. **MATCH_END**: Fin del partido. El `live_feed_producer` termina con exit code 0.

---

## Cómo verificar que funciona correctamente

### Logs esperados por servicio

**kafka-init:**
```
[kafka-init] Creando topic match_events...
Created topic match_events.
[kafka-init] Topic creado correctamente.
```

**live_feed_producer:**
```
[PRODUCER] Conectado a Kafka
[PRODUCER] Evento enviado: KICKOFF { match_id: '123', ... }
[PRODUCER] Evento enviado: GOAL { match_id: '123', team: 'home', ... }
```

**match_state_service:**
```
[STATE] Partido 123 iniciado
[STATE] GOL de home | Marcador: 1-0
[STATE] GOL ANULADO de home | Marcador: 0-0
[RABBIT] Publicado en live_updates | key=score.match_123
```

**betting_worker:**
```
[BETTING-WORKER] [PAUSA]    Comando: SUSPEND_BETS | match_id: 123 | motivo: GOAL
[BETTING-WORKER] [REANUDAR] Comando: RESUME_BETS  | match_id: 123 | motivo: GOAL_ANNULLED
```

**dashboard_backend:**
```
[WS] Cliente conectado: ::1 | Total: 1
[RABBIT] Mensaje recibido | routing_key: score.match_123
[WS] Broadcast enviado a 1 cliente(s)
```

### En el navegador (dashboard.html)
- El marcador cambia de `0:0` a `1:0` al producirse el gol
- Aparece un banner naranja parpadeando "VAR EN REVISIÓN" durante la espera
- El marcador vuelve a `0:0` con animación roja cuando el gol es anulado

### En el panel RabbitMQ (localhost:15672)
- **Exchanges**: `live_updates` (topic) y `live_alerts` (fanout) visibles
- **Queues**: `dashboard_q`, `alerts_q` y `betting_commands` con mensajes procesados

---

## Estructura del proyecto

```
var-live-score-platform/
├── docker-compose.yml              # Orquestación de todos los servicios
├── .env                            # Variables de entorno (Kafka, RabbitMQ, etc.)
├── scripts/
│   └── create-topics.sh           # Script que crea el topic de Kafka al arrancar
├── services/
│   ├── live_feed_producer/        # Productor: simula el partido en Kafka
│   ├── match_state_service/       # Marcador: consume Kafka, publica a RabbitMQ
│   ├── betting_suspension_service/# Apuestas: suspende/reanuda según eventos
│   ├── betting_worker/            # Worker: ejecuta comandos de apuestas
│   └── historical_archiver/       # Archivador: persiste todos los eventos
├── dashboard_backend/
│   ├── index.js                   # Servidor WebSocket + HTTP (puerto 3003)
│   └── setup-exchanges.js         # Declara la topología RabbitMQ
├── notification_backend/
│   └── index.js                   # Consumer de alertas (goles, VAR)
└── dashboard/
    └── dashboard.html             # Interfaz del marcador en tiempo real
```
