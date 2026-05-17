require("dotenv").config();

module.exports = {
  kafkaBroker: process.env.KAFKA_BROKER || "kafka:9092",
  kafkaTopic: process.env.KAFKA_TOPIC || "match_events",
  stateGroupId: process.env.STATE_GROUP_ID || "state-group",
  rabbitmqUrl: process.env.RABBITMQ_URL || "amqp://guest:guest@rabbitmq:5672",
};