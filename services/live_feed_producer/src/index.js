const { producer } = require("./kafka");
const { runScenario } = require("./scenario");

async function start() {
  try {
    await producer.connect();
    console.log("[PRODUCER] Conectado a Kafka");

    await runScenario();
  } catch (error) {
    console.error("[PRODUCER] Error:", error);
  } finally {
    await producer.disconnect();
    console.log("[PRODUCER] Desconectado de Kafka");
  }
}

start();