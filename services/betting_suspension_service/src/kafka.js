const { Kafka, logLevel } = require("kafkajs");
const { kafkaBroker, bettingGroupId } = require("./config");

const kafka = new Kafka({
  clientId: "betting-suspension-service",
  brokers: [kafkaBroker],
  logLevel: logLevel.INFO,
});

const consumer = kafka.consumer({ groupId: bettingGroupId });

module.exports = { consumer };