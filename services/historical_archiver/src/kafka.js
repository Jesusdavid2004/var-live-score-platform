// =============================================================================
// historical_archiver/src/kafka.js — Cliente Kafka del archivador histórico
// =============================================================================
// Crea y exporta el consumer de KafkaJS para el historical_archiver.
// Sigue el mismo patrón Singleton que todos los demás servicios Kafka del sistema:
// una sola instancia compartida en lugar de crear múltiples conexiones.
//
// ¿Qué hace diferente a este consumer de los otros dos (match_state y betting)?
// Cada consumer usa un groupId diferente:
//   - "state-group"               → match_state_service (actualiza marcador)
//   - "betting-group"             → betting_suspension_service (suspende apuestas)
//   - "historical-archiver-group" → historical_archiver (archiva todo)
//
// Al tener su propio groupId, Kafka garantiza que este servicio reciba
// TODOS los mensajes del topic "match_events" de forma independiente.
// No importa si los otros grupos ya leyeron esos mensajes: el archiver
// también los recibirá y los archivará completos.
//
// Analogía: es como tres personas leyendo el mismo libro al mismo tiempo.
// Cada una tiene su propio marcador (offset) y avanza a su propio ritmo.
// Si una persona se queda dormida (servicio se reinicia), las otras no se ven afectadas.
// =============================================================================

// KafkaJS: librería cliente de Kafka para Node.js
const { Kafka, logLevel } = require("kafkajs");

// Importamos la configuración: broker y groupId desde la config centralizada
// kafkaBroker:    dirección del broker (ej: "kafka:9092" en Docker)
// archiverGroupId: ID del grupo (ej: "historical-archiver-group")
const { kafkaBroker, archiverGroupId } = require("./config");

// =============================================================================
// Instancia del cliente Kafka
// =============================================================================
// El clientId "historical-archiver" es el nombre visible en los logs del broker
// y en herramientas de administración como Kafka UI. Ayuda a identificar
// este servicio específico cuando múltiples consumers están activos.
const kafka = new Kafka({
  clientId: "historical-archiver",  // Nombre visible en logs del broker Kafka
  brokers: [kafkaBroker],           // Array de brokers: en producción habría varios para HA
  logLevel: logLevel.INFO,          // INFO: muestra conexiones y errores, no mensajes individuales
});

// =============================================================================
// Consumer con Consumer Group "historical-archiver-group"
// =============================================================================
// groupId propio: Kafka mantiene un offset separado para este grupo.
//
// ¿Qué es el offset en Kafka?
// Cada mensaje en un topic tiene un número secuencial (offset): 0, 1, 2, 3...
// Kafka recuerda hasta qué offset procesó cada Consumer Group.
// Si el historical_archiver procesa hasta el offset 50 y luego se reinicia,
// cuando vuelve Kafka le entrega desde el offset 51 automáticamente.
// Esto garantiza que ningún evento se archive dos veces ni se pierda.
const consumer = kafka.consumer({
  groupId: archiverGroupId, // "historical-archiver-group": independiente de los otros grupos
});

// Exportamos solo el consumer: es lo único que necesita archiverService.js
module.exports = { consumer };
