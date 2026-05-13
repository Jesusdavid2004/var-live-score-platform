const { producer } = require("./kafka");
const { kafkaTopic } = require("./config");

async function publishEvent(event) {
  await producer.send({
    topic: kafkaTopic,
    messages: [
      {
        key: event.match_id,
        value: JSON.stringify(event),
      },
    ],
  });

  console.log(`[PRODUCER] Evento enviado: ${event.event_type}`, event);
}

module.exports = { publishEvent };