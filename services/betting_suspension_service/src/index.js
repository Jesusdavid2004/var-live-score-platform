// =============================================================================
// betting_suspension_service/src/index.js — Servicio de suspensión de apuestas
// =============================================================================
// Este servicio simula el sistema de gestión de apuestas en vivo de una casa
// de apuestas deportivas. Escucha los eventos del partido y decide cuándo
// suspender o reanudar las apuestas según la situación del juego.
//
// ¿Por qué existe este servicio separado?
// En sistemas reales (Bet365, William Hill, etc.), las apuestas en vivo deben
// suspenderse INMEDIATAMENTE cuando ocurre un gol o entra el VAR, para evitar
// que alguien apueste con información privilegiada (ya vio el gol en TV pero
// el sistema de apuestas aún no lo procesó). Este servicio simula esa lógica.
//
// ¿Cómo se integra con el resto del sistema?
// Este servicio es un consumidor PARALELO al match_state_service: ambos leen
// el mismo topic de Kafka pero con groupIds diferentes, por lo que cada uno
// recibe una copia independiente de todos los mensajes.
//
// Flujo de datos:
//   live_feed_producer → Kafka "match_events" → ESTE SERVICIO
//   → evaluateBettingAction() → publishBettingCommand()
//   → RabbitMQ "betting_commands" (Work Queue) → betting_worker
// =============================================================================

// consumer: instancia singleton del consumidor Kafka para este servicio
const { consumer } = require("./kafka");

// kafkaTopic: nombre del topic de Kafka del que se consumen los eventos
const { kafkaTopic } = require("./config");

// evaluateBettingAction: aplica las reglas de negocio de apuestas.
// Decide si un evento requiere SUSPEND_BETS, RESUME_BETS o ninguna acción.
const { evaluateBettingAction } = require("./suspensionLogic");

// connectRabbit: establece la conexión con RabbitMQ al arrancar.
// publishBettingCommand: publica el comando de apuestas a la Work Queue.
const {
  connect: connectRabbit,
  publishBettingCommand,
} = require("./rabbitmqPublisher");

// =============================================================================
// FUNCIÓN ASYNC: start()
// =============================================================================
// Arranca el servicio conectando primero a RabbitMQ (canal de salida) y
// luego a Kafka (canal de entrada). Este orden es intencional:
// si conectáramos Kafka primero, podríamos procesar eventos antes de estar
// listos para publicar los comandos resultantes.
// =============================================================================
async function start() {
  try {
    // Paso 1: Conectar a RabbitMQ y declarar la cola "betting_commands".
    // Si RabbitMQ no está disponible, lanza excepción → Docker reinicia el contenedor.
    await connectRabbit();
    console.log("[BETTING-SUSPENSION] Conectado a RabbitMQ");

    // Paso 2: Conectar el consumer al broker de Kafka
    await consumer.connect();

    // Paso 3: Suscribirse al topic de eventos del partido.
    // fromBeginning: true garantiza que al arrancar procesemos todos los eventos
    // históricos del topic, no solo los nuevos.
    await consumer.subscribe({ topic: kafkaTopic, fromBeginning: true });
    console.log("[BETTING-SUSPENSION] Suscrito a Kafka topic:", kafkaTopic);

    // Paso 4: Loop de consumo infinito.
    // KafkaJS llama a eachMessage() automáticamente por cada evento nuevo.
    await consumer.run({
      eachMessage: async ({ message }) => {
        // Convertimos el Buffer binario del mensaje a string JSON
        const rawValue = message.value?.toString() || "{}";

        let event;
        try {
          event = JSON.parse(rawValue); // String JSON → objeto JavaScript
        } catch {
          // Mensaje malformado: lo ignoramos y continuamos con el siguiente
          console.error("[BETTING-SUSPENSION] Error parseando evento:", rawValue);
          return;
        }

        console.log("[BETTING-SUSPENSION] Recibido:", event.event_type, event);

        // Aplicamos las reglas de negocio: ¿este evento requiere acción sobre apuestas?
        // evaluateBettingAction() devuelve un comando o null si no hay acción necesaria.
        const command = evaluateBettingAction(event);

        // Solo publicamos a RabbitMQ si el evento requiere acción.
        // Ej: FOUL no genera acción, GOAL sí genera SUSPEND_BETS.
        if (command) {
          publishBettingCommand(command);
        }
      },
    });

  } catch (error) {
    // Error al arrancar (Kafka/RabbitMQ no disponibles):
    // terminamos con código 1 para que Docker reinicie el contenedor
    console.error("[BETTING-SUSPENSION] Error:", error);
    process.exit(1);
  }
}

// Iniciamos el servicio al ejecutar este archivo
start();
