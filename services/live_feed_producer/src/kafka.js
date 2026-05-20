// ============================================================
// kafka.js — Inicialización del cliente Kafka (productor)
// ============================================================
// Este módulo crea la instancia de KafkaJS y exporta el objeto
// producer listo para ser conectado y usado. Al encapsularlo
// aquí, cualquier otro módulo importa el mismo productor
// (patrón singleton) en lugar de crear múltiples conexiones.
// ============================================================

// KafkaJS es la librería cliente de Kafka para Node.js.
// "logLevel" permite controlar cuánta información interna
// de KafkaJS aparece en la consola.
const { Kafka, logLevel } = require("kafkajs");

// Importamos broker y clientId desde la configuración centralizada
const { kafkaBroker, kafkaClientId } = require("./config");

// ------------------------------------------------------------
// Creación de la instancia Kafka
// - clientId: nombre con el que este servicio se identifica
//   ante el broker. Aparece en los logs de Kafka.
// - brokers: array con las direcciones de los brokers.
//   Aunque aquí solo hay uno, KafkaJS acepta varios para
//   alta disponibilidad en producción.
// - logLevel: INFO muestra conexiones y errores importantes
//   sin saturar la consola con mensajes de debug.
// ------------------------------------------------------------
const kafka = new Kafka({
  clientId: kafkaClientId,
  brokers: [kafkaBroker],
  logLevel: logLevel.INFO,
});

// Creamos el productor. Este objeto se conecta a Kafka y expone
// el método .send() para publicar mensajes en los topics.
// La conexión real ocurre cuando se llama producer.connect()
// en index.js; aquí solo se instancia.
const producer = kafka.producer();

// Exportamos el productor para que index.js lo conecte y
// publisher.js lo use para enviar mensajes.
module.exports = { producer };
