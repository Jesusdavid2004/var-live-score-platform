const { randomUUID } = require("crypto");

function nowIso() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function makeEventId(prefix) {
  return `${prefix}_${randomUUID().slice(0, 8)}`;
}

module.exports = { nowIso, sleep, makeEventId };