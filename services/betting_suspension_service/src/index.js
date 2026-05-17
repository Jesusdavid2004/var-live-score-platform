const { consumer } = require("./kafka");
const { kafkaTopic } = require("./config");
const { evaluateBettingAction } = require("./suspensionLogic");
const {
  connect: connectRabbit,
  publishBettingCommand,
} = require("./rabbitmqPublisher");

async function start() {
  try {
    await connectRabbit();
    console.log("[BETTING-SUSPENSION] Conectado a RabbitMQ");

    await consumer.connect();
    await consumer.subscribe({ topic: kafkaTopic, fromBeginning: true });
    console.log("[BETTING-SUSPENSION] Suscrito a Kafka topic:", kafkaTopic);

    await consumer.run({
      eachMessage: async ({ message }) => {
        const rawValue = message.value?.toString() || "{}";
        let event;
        try {
          event = JSON.parse(rawValue);
        } catch {
          console.error("[BETTING-SUSPENSION] Error parseando evento:", rawValue);
          return;
        }

        console.log("[BETTING-SUSPENSION] Recibido:", event.event_type, event);

        const command = evaluateBettingAction(event);
        if (command) {
          publishBettingCommand(command);
        }
      },
    });
  } catch (error) {
    console.error("[BETTING-SUSPENSION] Error:", error);
    process.exit(1);
  }
}

start();