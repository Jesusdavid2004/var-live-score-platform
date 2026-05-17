require("dotenv").config();

module.exports = {
  rabbitmqUrl: process.env.RABBITMQ_URL || "amqp://guest:guest@rabbitmq:5672",
};