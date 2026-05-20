// ============================================================
// index.js — Punto de entrada del match_state_service
// ============================================================
// Este servicio es el "árbitro digital" del sistema: consume
// eventos de Kafka, actualiza el marcador y publica el resultado
// a RabbitMQ para que el dashboard lo muestre en tiempo real.
//
// Flujo de datos:
//   Kafka (match_events) → processEvent() → RabbitMQ (live_updates)
//                                         → RabbitMQ (live_alerts)  [solo goles/VAR]
// ============================================================

// Importamos el consumer de Kafka (ya instanciado como singleton)
const { consumer } = require("./kafka");

// Importamos el nombre del topic desde la configuración
const { kafkaTopic } = require("./config");

// Motor de estado: procesa cada evento y actualiza el marcador
const { processEvent } = require("./stateManager");

// Publicador RabbitMQ: envía actualizaciones al dashboard y alertas
const {
  connect: connectRabbit,
  publishScoreUpdate,
  publishAlert,
} = require("./rabbitmqPublisher");

// ------------------------------------------------------------
// start()
// Orquesta el arranque completo del servicio:
//   1. Conecta a RabbitMQ primero (necesario para publicar después)
//   2. Conecta el consumer a Kafka
//   3. Se suscribe al topic de eventos
//   4. Inicia el loop de consumo
// ------------------------------------------------------------
async function start() {
  try {
    // Paso 1: Conectamos a RabbitMQ antes de empezar a consumir Kafka.
    // Si esto falla, no tiene sentido procesar eventos que no podríamos publicar.
    await connectRabbit();
    console.log("[MATCH-STATE] Conectado a RabbitMQ");

    // Paso 2: Conectamos el consumer al broker de Kafka
    await consumer.connect();

    // Paso 3: Nos suscribimos al topic de eventos del partido.
    // fromBeginning: true → la primera vez, lee todos los mensajes
    // desde el inicio del topic para reconstruir el estado completo.
    await consumer.subscribe({ topic: kafkaTopic, fromBeginning: true });
    console.log("[MATCH-STATE] Suscrito a Kafka topic:", kafkaTopic);

    // Paso 4: Iniciamos el loop de consumo. Cada mensaje llama a eachMessage.
    await consumer.run({
      eachMessage: async ({ message }) => {
        // Convertimos el valor del mensaje de Buffer a string JSON
        const rawValue = message.value?.toString() || "{}";

        // Parseamos el JSON del evento
        let event;
        try {
          event = JSON.parse(rawValue);
        } catch {
          // Si el mensaje no es JSON válido, lo ignoramos y seguimos
          console.error("[MATCH-STATE] Error parseando evento:", rawValue);
          return;
        }

        console.log("[MATCH-STATE] Recibido:", event.event_type, event);

        // Aplicamos la lógica de negocio: actualizamos el marcador
        // processEvent retorna el estado actualizado { match_id, home, away, event_type }
        const updatedState = processEvent(event);

        // Si processEvent no devuelve estado, saltamos la publicación
        if (!updatedState) return;

        // Publicamos el marcador actualizado al dashboard via RabbitMQ
        publishScoreUpdate(event.match_id, updatedState);

        // Mapa de alertas: solo algunos eventos generan alerta
        // (goles y decisiones VAR son los eventos más importantes)
        const alertMap = {
          GOAL:          `!GOL del equipo ${event.team}!`,
          GOAL_ANNULLED: "!GOL ANULADO POR EL VAR!",
          VAR_CHECK:     `VAR en revision para equipo ${event.team}`,
        };

        // Si el evento actual tiene una alerta asociada, la publicamos
        if (alertMap[event.event_type]) {
          // Los goles anulados son más urgentes (warning); el resto es info
          const severity = event.event_type === "GOAL_ANNULLED" ? "warning" : "info";
          publishAlert(alertMap[event.event_type], severity);
        }
      },
    });

  } catch (error) {
    // Error crítico: lo registramos y salimos con código 1
    // para que Docker reinicie el contenedor automáticamente
    console.error("[MATCH-STATE] Error:", error);
    process.exit(1);
  }
}

// Iniciamos el servicio al ejecutar este archivo
start();
