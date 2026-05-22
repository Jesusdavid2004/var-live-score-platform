// ============================================================
// index.js — Punto de entrada del live_feed_producer
// ============================================================
// Ciclo de vida:
//   1. Conecta a Kafka
//   2. Corre el partido completo UNA sola vez
//   3. Se queda esperando un mensaje { "command": "RESTART" }
//      en la cola RabbitMQ "restart_commands"
//   4. Al recibirlo, vuelve a correr el partido desde KICKOFF
//   5. Repite desde el paso 3 indefinidamente
//
// Esto permite que el botón "Reiniciar Partido" del dashboard
// controle exactamente cuándo empieza cada partido, sin tener
// que reiniciar el contenedor Docker.
// ============================================================

const amqp    = require("amqplib");
const { producer } = require("./kafka");
const { runScenario } = require("./scenario");
const { sleep } = require("./utils");
const { rabbitmqUrl } = require("./config");

const RESTART_QUEUE = "restart_commands";

// ------------------------------------------------------------
// esperarReinicio()
// Bloquea hasta recibir { "command": "RESTART" } en la cola
// restart_commands de RabbitMQ. Reintenta la conexión cada 5s
// si RabbitMQ no está disponible o se cae.
// ------------------------------------------------------------
async function esperarReinicio() {
  while (true) {
    let conn;
    try {
      conn = await amqp.connect(rabbitmqUrl);
      const ch = await conn.createChannel();
      await ch.assertQueue(RESTART_QUEUE, { durable: true });

      console.log("[PRODUCER] Esperando señal de reinicio en restart_commands...");

      await new Promise((resolve, reject) => {
        conn.on("close", () => reject(new Error("RabbitMQ conexion cerrada")));
        conn.on("error", reject);

        ch.consume(RESTART_QUEUE, (msg) => {
          if (!msg) return;
          try {
            const data = JSON.parse(msg.content.toString());
            if (data.command === "RESTART") {
              ch.ack(msg);
              resolve();
            } else {
              ch.ack(msg);
            }
          } catch (_) {
            ch.nack(msg, false, false);
          }
        });
      });

      await ch.close().catch(() => {});
      await conn.close().catch(() => {});
      return; // señal recibida → salir y correr el partido

    } catch (err) {
      console.error("[PRODUCER] Error esperando reinicio:", err.message);
      if (conn) await conn.close().catch(() => {});
      console.log("[PRODUCER] Reintentando conexión a RabbitMQ en 5s...");
      await sleep(5000);
    }
  }
}

// ------------------------------------------------------------
// start()
// Orquesta el ciclo completo: conecta Kafka, corre el primer
// partido y luego espera señales de reinicio indefinidamente.
// ------------------------------------------------------------
async function start() {
  try {
    await producer.connect();
    console.log("[PRODUCER] Conectado a Kafka");

    // Primer partido al arrancar
    console.log("[PRODUCER] ── Iniciando primer partido ──");
    await runScenario();
    console.log("[PRODUCER] Partido finalizado. Esperando reinicio...");

    // Bucle: esperar RESTART → correr partido → repetir
    while (true) {
      await esperarReinicio();
      console.log("[PRODUCER] ── Reiniciando partido ──");
      await runScenario();
      console.log("[PRODUCER] Partido finalizado. Esperando reinicio...");
    }

  } catch (error) {
    console.error("[PRODUCER] Error fatal:", error);
  } finally {
    await producer.disconnect();
    console.log("[PRODUCER] Desconectado de Kafka");
  }
}

start();
