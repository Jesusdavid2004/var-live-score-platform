// ============================================================
// index.js — Punto de entrada del betting_suspension_service
// ============================================================
// Este servicio escucha todos los eventos del partido desde Kafka
// y decide si debe suspender o reanudar las apuestas en vivo,
// publicando los comandos correspondientes en RabbitMQ.
//
// Flujo de datos:
//   Kafka (match_events) → evaluateBettingAction() → RabbitMQ (betting_commands)
//
// Es un consumidor paralelo al match_state_service: ambos leen
// el mismo topic de Kafka pero cada uno tiene su propia lógica
// y su propio grupo de consumidores.
// ============================================================

// Consumer de Kafka instanciado como singleton
const { consumer } = require("./kafka");

// Nombre del topic del que consumimos eventos
const { kafkaTopic } = require("./config");

// Módulo con la lógica de decisión de apuestas
const { evaluateBettingAction } = require("./suspensionLogic");

// Módulo que publica los comandos a RabbitMQ
const {
  connect: connectRabbit,
  publishBettingCommand,
} = require("./rabbitmqPublisher");

// ------------------------------------------------------------
// start()
// Arranca el servicio en el orden correcto:
//   1. Conecta a RabbitMQ (necesario antes de publicar comandos)
//   2. Conecta a Kafka y se suscribe al topic
//   3. Procesa cada mensaje aplicando la lógica de suspensión
// ------------------------------------------------------------
async function start() {
  try {
    // Primero RabbitMQ: si no podemos publicar, no tiene sentido consumir
    await connectRabbit();
    console.log("[BETTING-SUSPENSION] Conectado a RabbitMQ");

    // Conectamos el consumer al broker de Kafka
    await consumer.connect();

    // Nos suscribimos al topic. fromBeginning: true asegura que
    // procesemos todos los eventos desde el inicio del topic.
    await consumer.subscribe({ topic: kafkaTopic, fromBeginning: true });
    console.log("[BETTING-SUSPENSION] Suscrito a Kafka topic:", kafkaTopic);

    // Loop de consumo: se ejecuta indefinidamente hasta que el
    // contenedor Docker se detenga
    await consumer.run({
      eachMessage: async ({ message }) => {
        // Convertimos el mensaje de Buffer a string y lo parseamos
        const rawValue = message.value?.toString() || "{}";
        let event;
        try {
          event = JSON.parse(rawValue);
        } catch {
          console.error("[BETTING-SUSPENSION] Error parseando evento:", rawValue);
          return; // Ignoramos mensajes malformados
        }

        console.log("[BETTING-SUSPENSION] Recibido:", event.event_type, event);

        // Aplicamos las reglas de negocio para decidir el comando
        // evaluateBettingAction devuelve un comando o null
        const command = evaluateBettingAction(event);

        // Solo publicamos si el evento requiere una acción sobre las apuestas
        if (command) {
          publishBettingCommand(command);
        }
      },
    });

  } catch (error) {
    // Error crítico al arrancar: salimos con código 1 para
    // que Docker reinicie el contenedor automáticamente
    console.error("[BETTING-SUSPENSION] Error:", error);
    process.exit(1);
  }
}

// Iniciamos el servicio al ejecutar este archivo
start();
