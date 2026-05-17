const { consumer } = require("./kafka");
const { kafkaTopic } = require("./config");
const { processEvent } = require("./stateManager");
const {
  connect: connectRabbit,
  publishScoreUpdate,
  publishAlert,
} = require("./rabbitmqPublisher");

async function start() {
  try {
    await connectRabbit();
    console.log("[MATCH-STATE] Conectado a RabbitMQ");

    await consumer.connect();
    await consumer.subscribe({ topic: kafkaTopic, fromBeginning: true });
    console.log("[MATCH-STATE] Suscrito a Kafka topic:", kafkaTopic);

    await consumer.run({
      eachMessage: async ({ message }) => {
        const rawValue = message.value?.toString() || "{}";
        let event;
        try {
          event = JSON.parse(rawValue);
        } catch {
          console.error("[MATCH-STATE] Error parseando evento:", rawValue);
          return;
        }

        console.log("[MATCH-STATE] Recibido:", event.event_type, event);

        const updatedState = processEvent(event);

        if (!updatedState) return;

        publishScoreUpdate(event.match_id, updatedState);

        const alertMap = {
          GOAL: `!GOL del equipo ${event.team}!`,
          GOAL_ANNULLED: "!GOL ANULADO POR EL VAR!",
          VAR_CHECK: `VAR en revision para equipo ${event.team}`,
        };

        if (alertMap[event.event_type]) {
          const severity = event.event_type === "GOAL_ANNULLED" ? "warning" : "info";
          publishAlert(alertMap[event.event_type], severity);
        }
      },
    });
  } catch (error) {
    console.error("[MATCH-STATE] Error:", error);
    process.exit(1);
  }
}

start();