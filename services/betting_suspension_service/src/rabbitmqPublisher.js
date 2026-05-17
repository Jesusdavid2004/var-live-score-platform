const amqplib = require("amqplib");
const { rabbitmqUrl } = require("./config");

let channel = null;
let connection = null;

const QUEUE_BETTING = "betting_commands";

async function connect() {
  connection = await amqplib.connect(rabbitmqUrl);
  channel = await connection.createChannel();

  await channel.assertQueue(QUEUE_BETTING, { durable: true });

  console.log("[RABBIT] Queue declarada:", QUEUE_BETTING);
}

function publishBettingCommand(command) {
  const message = JSON.stringify(command);
  channel.sendToQueue(QUEUE_BETTING, Buffer.from(message), { persistent: true });
  console.log("[RABBIT] Comando publicado a", QUEUE_BETTING, command);
}

async function close() {
  if (channel) await channel.close();
  if (connection) await connection.close();
}

module.exports = { connect, publishBettingCommand, close };