require("dotenv").config();

module.exports = {
  kafkaBroker: process.env.KAFKA_BROKER || "kafka:9092",
  kafkaTopic: process.env.KAFKA_TOPIC || "match_events",
  kafkaClientId: process.env.KAFKA_CLIENT_ID || "live-feed-producer",
  matchId: process.env.MATCH_ID || "123",
  varDelayMs: Number(process.env.VAR_DELAY_MS || 20000),
};