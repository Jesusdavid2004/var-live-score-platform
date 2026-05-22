// =============================================================================
// betting_suspension_service/src/kafka.js — Cliente Kafka del servicio de apuestas
// =============================================================================
// Crea el consumer de KafkaJS para el servicio de suspensión de apuestas.
// Sigue el mismo patrón Singleton que los demás servicios del sistema:
// una sola instancia de consumer compartida por todos los módulos.
//
// ¿Qué hace diferente a este consumer del de match_state_service?
// El groupId "betting-group" es distinto de "state-group".
// Esto es fundamental en Kafka: cada Consumer Group es independiente.
//
// Ejemplo para entender el aislamiento de grupos:
//   Topic "match_events" tiene mensajes: [KICKOFF, GOAL, VAR_CHECK, GOAL_ANNULLED]
//
//   Grupo "state-group" (match_state_service):
//     Lee: KICKOFF, GOAL, VAR_CHECK, GOAL_ANNULLED (todos, offset propio)
//
//   Grupo "betting-group" (betting_suspension_service):
//     Lee: KICKOFF, GOAL, VAR_CHECK, GOAL_ANNULLED (los mismos, offset propio)
//
//   Grupo "historical-archiver-group" (historical_archiver):
//     Lee: KICKOFF, GOAL, VAR_CHECK, GOAL_ANNULLED (los mismos, offset propio)
//
// Los tres grupos leen el MISMO contenido pero son 100% independientes:
// si uno de los servicios se reinicia, no afecta a los otros ni pierde mensajes.
// =============================================================================

// KafkaJS: librería cliente de Kafka para Node.js
const { Kafka, logLevel } = require("kafkajs");

// Importamos la configuración centralizada de este servicio
// kafkaBroker:   dirección del broker (ej: "kafka:9092" en Docker)
// bettingGroupId: ID del grupo de consumidores (ej: "betting-group")
const { kafkaBroker, bettingGroupId } = require("./config");

// Instancia del cliente Kafka.
// clientId "betting-suspension-service" identifica este servicio en los logs
// del broker de Kafka, facilitando el diagnóstico cuando hay múltiples servicios.
const kafka = new Kafka({
  clientId: "betting-suspension-service", // Identificador visible en los logs del broker
  brokers: [kafkaBroker],                 // Lista de brokers del cluster (solo uno en desarrollo)
  logLevel: logLevel.INFO,                // Nivel INFO: muestra conexiones y errores, no mensajes individuales
});

// Consumer con el grupo de apuestas.
// Al usar "betting-group", Kafka mantiene un offset (posición de lectura)
// completamente separado del grupo "state-group" del match_state_service.
// Esto significa que aunque el match_state_service procese un GOAL, el
// betting_suspension_service lo procesará de forma independiente después.
const consumer = kafka.consumer({ groupId: bettingGroupId });

// Exportamos el consumer para que index.js lo use al suscribirse al topic
module.exports = { consumer };
