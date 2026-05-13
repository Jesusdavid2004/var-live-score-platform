require("dotenv").config();

module.exports = {
  kafkaBroker: process.env.KAFKA_BROKER || "kafka:9092",
  kafkaTopic: process.env.KAFKA_TOPIC || "match_events",
  archiverGroupId:
    process.env.ARCHIVER_GROUP_ID || "historical-archiver-group",
};