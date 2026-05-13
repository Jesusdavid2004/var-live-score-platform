const { consumer } = require("./kafka");
const { kafkaTopic } = require("./config");

async function startArchiver() {
  await consumer.connect();
  await consumer.subscribe({ topic: kafkaTopic, fromBeginning: true });

  console.log("[ARCHIVER] Escuchando eventos...");

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const key = message.key?.toString() || null;
      const rawValue = message.value?.toString() || "{}";

      let payload;
      try {
        payload = JSON.parse(rawValue);
      } catch (error) {
        console.error("[ARCHIVER] Error parseando mensaje:", rawValue);
        return;
      }

      console.log("[ARCHIVER] Archivando evento:", {
        topic,
        partition,
        key,
        payload,
      });
    },
  });
}

module.exports = { startArchiver };