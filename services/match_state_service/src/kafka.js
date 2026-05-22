// =============================================================================
// match_state_service/src/kafka.js — Cliente Kafka del árbitro digital
// =============================================================================
// Este módulo crea y exporta el consumer de KafkaJS para el match_state_service.
// Usa el patrón Singleton: crea una sola instancia y la reutiliza en todo el
// servicio, evitando múltiples conexiones innecesarias al broker de Kafka.
//
// ¿Qué es un Consumer Group en Kafka?
// Kafka permite que múltiples servicios lean el mismo topic simultáneamente.
// Cada "Consumer Group" es independiente: Kafka mantiene un offset (posición
// de lectura) separado para cada grupo. Esto significa que:
//   - match_state_service (grupo "state-group") recibe TODOS los mensajes
//   - historical_archiver (grupo "historical-archiver-group") también recibe TODOS
//   - betting_suspension_service (grupo "betting-group") también recibe TODOS
// Los tres servicios leen el mismo topic de forma totalmente independiente.
//
// ¿Qué pasa si este servicio se reinicia?
// Kafka recuerda el offset del grupo "state-group". Al reiniciar, el consumer
// retoma desde donde quedó sin reprocesar mensajes ya procesados.
// La excepción es fromBeginning: true en consumer.subscribe(), que al arrancar
// por primera vez lee TODO el historial del topic para reconstruir el estado.
// =============================================================================

// KafkaJS: librería cliente de Kafka para Node.js
// logLevel: enum para controlar el nivel de verbosidad de los logs del cliente
const { Kafka, logLevel } = require("kafkajs");

// Importamos el broker y el groupId desde la configuración centralizada.
// kafkaBroker:  dirección del broker (ej: "kafka:9092" en Docker)
// stateGroupId: ID del grupo de consumidores (ej: "state-group")
const { kafkaBroker, stateGroupId } = require("./config");

// =============================================================================
// Instancia del cliente Kafka
// =============================================================================
// El clientId "match-state-service" es un identificador humano que aparece en:
//   - Los logs del broker de Kafka
//   - El panel de administración de Kafka UI (si está instalado)
//   - Los mensajes de error para facilitar el diagnóstico
// Es diferente del groupId: clientId identifica la conexión, groupId el grupo.
const kafka = new Kafka({
  clientId: "match-state-service",  // Nombre visible en los logs del broker
  brokers: [kafkaBroker],           // Array de brokers (en producción serían varios)
  logLevel: logLevel.INFO,          // Nivel de log: INFO muestra conexiones y errores
});

// =============================================================================
// Consumer con Consumer Group "state-group"
// =============================================================================
// Al crear el consumer con este groupId, Kafka lo identifica como parte del
// grupo "state-group". Si hubiera múltiples particiones en el topic, Kafka
// podría balancear automáticamente las particiones entre consumidores del mismo grupo.
// Con una sola partición (nuestra configuración), hay un consumer por grupo.
const consumer = kafka.consumer({ groupId: stateGroupId });

// Exportamos el consumer para que index.js lo use en el loop de consumo.
// Solo se exporta el consumer, no la instancia kafka, porque los otros módulos
// no necesitan crear producers ni administrar el cluster.
module.exports = { consumer };
