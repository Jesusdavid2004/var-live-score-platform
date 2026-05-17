const amqplib = require("amqplib");
const { rabbitmqUrl } = require("./config");

let channel = null;
let connection = null;

const EXCHANGE_TOPIC = "live_updates";
const EXCHANGE_FANOUT = "live_alerts";
const QUEUE_BETTING = "betting_commands";

async function connect() {
  connection = await amqplib.connect(rabbitmqUrl);
  channel = await connection.createChannel();

  await channel.assertExchange(EXCHANGE_TOPIC, "topic", { durable: true });
  await channel.assertExchange(EXCHANGE_FANOUT, "fanout", { durable: true });

  console.log("[RABBIT] Exchanges declarados: live_updates (topic), live_alerts (fanout)");
}

function publishScoreUpdate(matchId, state) {
  const routingKey = `score.match_${matchId}`;
  const message = JSON.stringify(state);
  channel.publish(EXCHANGE_TOPIC, routingKey, Buffer.from(message));
  console.log(`[RABBIT] Publicado en ${EXCHANGE_TOPIC} | key=${routingKey}`, state);
}

function publishAlert(message, severity) {
  const payload = JSON.stringify({ message, severity });
  channel.publish(EXCHANGE_FANOUT, "", Buffer.from(payload));
  console.log(`[RABBIT] Publicado en ${EXCHANGE_FANOUT} | severity=${severity}: ${message}`);
}

async function close() {
  if (channel) await channel.close();
  if (connection) await connection.close();
}

module.exports = { connect, publishScoreUpdate, publishAlert, close };