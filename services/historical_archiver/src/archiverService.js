// =============================================================================
// historical_archiver/src/archiverService.js — Lógica del archivador histórico
// =============================================================================
// Este módulo contiene toda la lógica del historical_archiver: conectarse a
// Kafka, suscribirse al topic de eventos y persistir cada mensaje recibido.
//
// ¿Cuál es el rol del historical_archiver en la arquitectura?
// Es el "notario" del sistema: registra TODOS los eventos que ocurren en el
// partido con sus metadatos completos de Kafka (topic, partición, offset, key).
// Esto es fundamental para:
//
//   1. AUDITORÍA: verificar exactamente qué eventos se publicaron y cuándo.
//      Si hay un error en el marcador, se puede revisar el historial para
//      encontrar el evento que causó el problema.
//
//   2. REPLAY: si se agrega una nueva regla de negocio (ej: nuevo tipo de apuesta),
//      se pueden reprocesar todos los eventos históricos para recalcular el estado.
//      Kafka permite leer el topic desde el principio (fromBeginning: true).
//
//   3. ANÁLISIS: contar goles por equipo, frecuencia de revisiones VAR, etc.
//      En producción, los eventos archivados se consultarían con SQL o un motor
//      de análisis como Apache Spark o AWS Athena.
//
// Diferencia clave con match_state_service:
//   - match_state_service: procesa eventos y MODIFICA el estado (marcador)
//   - historical_archiver: solo REGISTRA eventos sin modificar ningún estado
// =============================================================================

// consumer: instancia singleton del consumer de KafkaJS para este servicio
const { consumer } = require("./kafka");

// kafkaTopic: nombre del topic de Kafka del que se consumen los eventos
const { kafkaTopic } = require("./config");

// =============================================================================
// FUNCIÓN ASYNC: startArchiver()
// =============================================================================
// Conecta el consumer a Kafka y arranca el loop de archivado indefinido.
// Cada mensaje que llega al topic se registra con sus metadatos completos.
//
// ¿Por qué fromBeginning: true?
// Al arrancar por primera vez, queremos archivar TODOS los eventos históricos
// del topic, no solo los nuevos. Si el archiver se reinicia (ej: por un error),
// Kafka le entregará los eventos desde el último offset procesado, no desde 0,
// gracias al Consumer Group "historical-archiver-group" que guarda el progreso.
// =============================================================================
async function startArchiver() {
  // Establecemos la conexión TCP con el broker de Kafka.
  // Si Kafka no está disponible (ej: Docker aún iniciando), lanza excepción
  // que sube a bootstrap() en index.js y termina el proceso con código 1.
  await consumer.connect();

  // Nos suscribimos al topic de eventos del partido.
  // fromBeginning: true → al primera conexión, leer desde el mensaje más antiguo.
  // En reconexiones posteriores, Kafka entrega desde el último offset confirmado.
  await consumer.subscribe({ topic: kafkaTopic, fromBeginning: true });

  console.log("[ARCHIVER] Escuchando eventos...");

  // Iniciamos el loop de consumo. KafkaJS llama a eachMessage() por cada
  // mensaje nuevo que llega. El proceso no termina: espera mensajes indefinidamente.
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      // ── Extraemos los metadatos del mensaje de Kafka ──────────────────────
      // Estos metadatos son únicos de Kafka y no forman parte del payload del evento.
      // Son útiles para debugging, auditoría y para saber la posición en el log.

      // message.key: clave del mensaje (normalmente el match_id serializado).
      // El operador ?. evita error si key es null (mensajes sin key definida).
      const key = message.key?.toString() || null;

      // message.value: el payload del evento en formato Buffer binario.
      // Lo convertimos a string JSON para parsearlo.
      const rawValue = message.value?.toString() || "{}";

      // Parseamos el JSON del payload del evento
      let payload;
      try {
        payload = JSON.parse(rawValue); // String JSON → objeto JavaScript
      } catch (error) {
        // Si el mensaje no es JSON válido (error del productor), lo registramos
        // pero NO crasheamos: continuamos con el siguiente mensaje.
        console.error("[ARCHIVER] Error parseando mensaje:", rawValue);
        return; // Saltamos este mensaje específico
      }

      // ── Archivamos el evento con todos sus metadatos ──────────────────────
      // En producción, esta línea sería reemplazada por:
      //   await db.insert('events', { topic, partition, key, payload, archivedAt: new Date() })
      // o:
      //   await s3.upload({ Bucket: 'match-events', Key: `${key}/${payload.event_id}.json`, Body: rawValue })
      //
      // Los campos registrados:
      //   topic:     el topic de Kafka del que vino (útil si el archiver consume múltiples topics)
      //   partition: en qué partición estaba almacenado el mensaje (0 en nuestra config)
      //   key:       la clave del mensaje = match_id (para filtrar eventos por partido)
      //   payload:   el objeto del evento con event_type, home, away, team, timestamp, etc.
      console.log("[ARCHIVER] Archivando evento:", {
        topic,     // Nombre del topic: "match_events"
        partition, // Número de partición: 0 (solo hay una en nuestra configuración)
        key,       // match_id del partido: "123"
        payload,   // El evento completo: { match_id, event_type, event_id, timestamp, ... }
      });
    },
  });
}

// Exportamos startArchiver para que index.js la llame al arrancar el servicio
module.exports = { startArchiver };
