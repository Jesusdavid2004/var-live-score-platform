const amqplib = require("amqplib");
const { rabbitmqUrl } = require("./config");

const QUEUE_BETTING = "betting_commands";

async function start() {
  const connection = await amqplib.connect(rabbitmqUrl);
  const channel = await connection.createChannel();

  await channel.assertQueue(QUEUE_BETTING, { durable: true });
  await channel.prefetch(1);

  console.log("[BETTING-WORKER] Esperando comandos en", QUEUE_BETTING);

  channel.consume(
    QUEUE_BETTING,
    (msg) => {
      if (!msg) return;

      const content = msg.content.toString();
      let command;
      try {
        command = JSON.parse(content);
      } catch {
        console.error("[BETTING-WORKER] Error parseando comando:", content);
        channel.nack(msg, false, false);
        return;
      }

      const icon = command.command === "SUSPEND_BETS" ? "PAUSA" : "REANUDAR";

      console.log(
        `[BETTING-WORKER] [${icon}] Comando: ${command.command} | match_id: ${command.match_id} | motivo: ${command.reason}`
      );

      channel.ack(msg);
    },
    { noAck: false }
  );
}

start().catch((err) => {
  console.error("[BETTING-WORKER] Error:", err);
  process.exit(1);
});