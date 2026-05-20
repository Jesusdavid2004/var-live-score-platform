// ============================================================
// kafka.js — Cliente Kafka del historical_archiver (consumidor)
// ============================================================
// Crea y exporta el consumer de KafkaJS. Al igual que en el
// productor, se usa el patrón singleton: un solo objeto consumer
// es compartido por todos los módulos que lo necesiten.
// ============================================================

// Importamos KafkaJS y el nivel de log para configurar el cliente
const { Kafka, logLevel } = require("kafkajs");

// Importamos broker y groupId desde la configuración centralizada
const { kafkaBroker, archiverGroupId } = require("./config");

// ------------------------------------------------------------
// Instancia del cliente Kafka
// clientId "historical-archiver" identifica este servicio en los
// logs del broker. Ayuda a distinguirlo del match_state_service
// y del betting_suspension_service cuando todos consumen el mismo topic.
// ------------------------------------------------------------
const kafka = new Kafka({
  clientId: "historical-archiver",
  brokers: [kafkaBroker],
  logLevel: logLevel.INFO,
});

// ------------------------------------------------------------
// Consumer con groupId propio
// Al usar "historical-archiver-group", Kafka lleva un offset
// (posición) independiente para este servicio. Si el archiver
// se reinicia, retoma desde el último mensaje que procesó,
// sin perder ni duplicar eventos.
// ------------------------------------------------------------
const consumer = kafka.consumer({
  groupId: archiverGroupId,
});

// Exportamos el consumer para que archiverService.js lo use
module.exports = { consumer };
