const { Kafka, logLevel } = require("kafkajs");
const { kafkaBroker, stateGroupId } = require("./config");

const kafka = new Kafka({
  clientId: "match-state-service",
  brokers: [kafkaBroker],
  logLevel: logLevel.INFO,
});

const consumer = kafka.consumer({ groupId: stateGroupId });

module.exports = { consumer };