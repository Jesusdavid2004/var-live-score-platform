const { Kafka, logLevel } = require("kafkajs");
const { kafkaBroker, kafkaClientId } = require("./config");

const kafka = new Kafka({
  clientId: kafkaClientId,
  brokers: [kafkaBroker],
  logLevel: logLevel.INFO,
});

const producer = kafka.producer();

module.exports = { producer };