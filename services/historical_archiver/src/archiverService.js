// ============================================================
// archiverService.js — Lógica principal del archivador histórico
// ============================================================
// Este servicio actúa como el "registrador" del sistema: consume
// TODOS los eventos que llegan al topic de Kafka y los persiste
// (en este caso, en consola; en producción sería una base de datos
// como PostgreSQL, MongoDB o S3).
//
// ¿Por qué existe un archivador separado?
// En sistemas distribuidos es una buena práctica tener un servicio
// dedicado a persistir todos los eventos. Esto permite:
//   - Auditoría: saber exactamente qué pasó y cuándo
//   - Replay: reprocesar eventos históricos si cambia la lógica
//   - Debugging: comparar lo que se publicó vs lo que se procesó
// ============================================================

// Importamos el consumer ya instanciado desde kafka.js
const { consumer } = require("./kafka");

// Importamos el nombre del topic desde la configuración
const { kafkaTopic } = require("./config");

// ------------------------------------------------------------
// startArchiver()
// Conecta el consumer a Kafka, se suscribe al topic y empieza
// a escuchar mensajes indefinidamente (el proceso no termina).
//
// fromBeginning: true → al primera vez que arranca, lee todos
// los mensajes desde el inicio del topic, no solo los nuevos.
// Esto garantiza que el archivador tenga un historial completo
// aunque haya arrancado tarde.
// ------------------------------------------------------------
async function startArchiver() {
  // Establece la conexión con el broker de Kafka
  await consumer.connect();

  // Se suscribe al topic. fromBeginning: true asegura que el archivador
  // procese todos los eventos desde el offset 0 la primera vez.
  await consumer.subscribe({ topic: kafkaTopic, fromBeginning: true });

  console.log("[ARCHIVER] Escuchando eventos...");

  // Inicia el loop de consumo. eachMessage se llama una vez por
  // cada mensaje que Kafka entrega al consumer.
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      // Extraemos la key del mensaje (es el match_id serializado como Buffer)
      // Si no tiene key, usamos null
      const key = message.key?.toString() || null;

      // Extraemos el valor del mensaje como string JSON
      const rawValue = message.value?.toString() || "{}";

      // Parseamos el JSON; si viene malformado, lo registramos y seguimos
      let payload;
      try {
        payload = JSON.parse(rawValue);
      } catch (error) {
        console.error("[ARCHIVER] Error parseando mensaje:", rawValue);
        return; // Saltamos este mensaje sin crashear el servicio
      }

      // Registramos el evento con todos sus metadatos de Kafka:
      //   - topic: el topic del que vino (útil si consume varios)
      //   - partition: en qué partición estaba el mensaje
      //   - key: el match_id (sirve para filtrar por partido después)
      //   - payload: el contenido del evento
      // En producción aquí iría: db.insert(payload) o s3.upload(payload)
      console.log("[ARCHIVER] Archivando evento:", {
        topic,
        partition,
        key,
        payload,
      });
    },
  });
}

// Exportamos la función para que index.js la llame al arrancar
module.exports = { startArchiver };
