const { Kafka, logLevel } = require("kafkajs");
const { kafkaBroker, archiverGroupId } = require("./config");

const kafka = new Kafka({
  clientId: "historical-archiver",
  brokers: [kafkaBroker],
  logLevel: logLevel.INFO,
});

const consumer = kafka.consumer({
  groupId: archiverGroupId,
});

module.exports = { consumer };