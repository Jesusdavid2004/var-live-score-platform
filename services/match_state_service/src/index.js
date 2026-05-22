// =============================================================================
// match_state_service/src/index.js — Punto de entrada del árbitro digital
// =============================================================================
// Este servicio es el "árbitro digital" del sistema VAR Live Score Platform.
// Es el componente más importante de la arquitectura porque:
//   1. CONSUME eventos crudos de Kafka (GOAL, VAR_CHECK, etc.)
//   2. APLICA la lógica de negocio (actualizar marcador, gestionar goles)
//   3. PUBLICA el resultado a RabbitMQ para que llegue al dashboard y alertas
//
// ¿Por qué existe un servicio separado para el estado del partido?
// En arquitecturas de microservicios, el estado de negocio no debe vivir
// en el productor de datos ni en el presentador (dashboard). Un servicio
// dedicado al estado permite: escalar el dashboard sin afectar la lógica,
// agregar nuevas reglas de negocio sin tocar el productor, y tener una
// fuente única de verdad para el marcador.
//
// Flujo de datos completo que maneja este servicio:
//   live_feed_producer → Kafka "match_events" → ESTE SERVICIO (procesa)
//   → RabbitMQ "live_updates" (topic)  → dashboard_backend → WebSocket → navegador
//   → RabbitMQ "live_alerts"  (fanout) → notification_backend → consola/push
// =============================================================================

// consumer: instancia del consumidor Kafka, creado en kafka.js como singleton.
// El patrón singleton garantiza una sola conexión activa con el broker.
const { consumer } = require("./kafka");

// kafkaTopic: nombre del topic de Kafka del que consumimos los eventos.
// Debe coincidir exactamente con el topic donde publica el live_feed_producer.
const { kafkaTopic } = require("./config");

// processEvent: función del motor de estado que aplica la lógica de negocio.
// Recibe un evento y devuelve el estado actualizado del partido (marcador).
const { processEvent } = require("./stateManager");

// connectRabbit: establece la conexión con RabbitMQ al arrancar el servicio.
// publishScoreUpdate: publica el marcador actualizado al dashboard.
// publishAlert: publica alertas de gol/VAR al sistema de notificaciones.
const {
  connect: connectRabbit,
  publishScoreUpdate,
  publishAlert,
} = require("./rabbitmqPublisher");

// =============================================================================
// FUNCIÓN ASYNC: start()
// =============================================================================
// Orquesta el arranque completo del servicio en el orden correcto:
//   Paso 1: RabbitMQ primero (necesitamos poder publicar antes de consumir)
//   Paso 2: Conexión Kafka
//   Paso 3: Suscripción al topic de eventos
//   Paso 4: Loop de consumo infinito (procesa cada evento del partido)
//
// ¿Por qué primero RabbitMQ y luego Kafka?
// Si conectamos Kafka primero y llegaran eventos antes de que RabbitMQ esté
// listo, los procesaríamos pero no podríamos publicar los resultados.
// Conectar RabbitMQ primero garantiza que el pipeline completo esté listo.
// =============================================================================
async function start() {
  try {
    // Paso 1: Conectar a RabbitMQ y declarar los exchanges.
    // Si RabbitMQ no está listo (Docker aún arrancando), lanza excepción
    // y el proceso termina con código 1 → Docker lo reiniciará automáticamente.
    await connectRabbit();
    console.log("[MATCH-STATE] Conectado a RabbitMQ");

    // Paso 2: Conectar el consumer al broker de Kafka.
    // Esta llamada establece la conexión TCP y el handshake con el broker.
    await consumer.connect();

    // Paso 3: Suscribirse al topic de eventos del partido.
    // fromBeginning: true → la PRIMERA VEZ que arranca, lee todos los mensajes
    // desde el inicio del topic, reconstruyendo el historial completo.
    // Las siguientes veces, Kafka recuerda el offset y continúa desde ahí.
    await consumer.subscribe({ topic: kafkaTopic, fromBeginning: true });
    console.log("[MATCH-STATE] Suscrito a Kafka topic:", kafkaTopic);

    // Paso 4: Iniciar el loop de consumo.
    // consumer.run() bloquea el proceso y llama a eachMessage() por cada
    // mensaje nuevo que llega al topic. No hay un while(true) explícito:
    // KafkaJS lo maneja internamente de forma eficiente.
    await consumer.run({
      eachMessage: async ({ message }) => {
        // message.value es un Buffer binario: lo convertimos a string JSON.
        // El operador ?. (optional chaining) evita errores si value es null.
        const rawValue = message.value?.toString() || "{}";

        // Parseamos el JSON del evento: si falla, ignoramos el mensaje.
        let event;
        try {
          event = JSON.parse(rawValue);
        } catch {
          // Un mensaje no-JSON no debería llegar al topic, pero si ocurre
          // lo descartamos y continuamos con el siguiente mensaje.
          console.error("[MATCH-STATE] Error parseando evento:", rawValue);
          return;
        }

        console.log("[MATCH-STATE] Recibido:", event.event_type, event);

        // Aplicamos la lógica de negocio al evento recibido.
        // processEvent() actualiza el marcador en memoria y retorna el nuevo estado.
        // El estado incluye: { match_id, home, away, event_type }
        const updatedState = processEvent(event);

        // Si processEvent no devuelve estado (evento desconocido), no publicamos.
        if (!updatedState) return;

        // Publicamos el marcador actualizado al Topic Exchange "live_updates".
        // El dashboard_backend lo recibirá y hará broadcast a todos los navegadores.
        publishScoreUpdate(event.match_id, updatedState);

        // Solo algunos eventos importantes generan alertas al sistema de notificaciones.
        // Las faltas (FOUL) y el inicio/fin del partido no generan alertas.
        const alertMap = {
          GOAL:          `!GOL del equipo ${event.team}!`,      // Gol anotado
          GOAL_ANNULLED: "!GOL ANULADO POR EL VAR!",            // VAR anuló el gol
          VAR_CHECK:     `VAR en revision para equipo ${event.team}`, // VAR en acción
        };

        // Solo publicamos alerta si este tipo de evento tiene una entrada en alertMap
        if (alertMap[event.event_type]) {
          // Los goles anulados son más críticos (warning): cambian el marcador hacia atrás.
          // El resto de eventos importantes son informativos (info).
          const severity = event.event_type === "GOAL_ANNULLED" ? "warning" : "info";
          publishAlert(alertMap[event.event_type], severity);
        }
      },
    });

  } catch (error) {
    // Error crítico al arrancar (ej: Kafka o RabbitMQ no disponibles).
    // Terminamos con código 1 para que Docker reinicie el contenedor.
    console.error("[MATCH-STATE] Error:", error);
    process.exit(1);
  }
}

// Iniciamos el servicio cuando Node.js ejecuta este archivo.
start();
