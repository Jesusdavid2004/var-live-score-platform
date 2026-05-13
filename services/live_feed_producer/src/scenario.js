const { matchId, varDelayMs } = require("./config");
const { sleep } = require("./utils");
const { publishEvent } = require("./publisher");
const {
  kickoff,
  goal,
  varCheck,
  goalAnnulled,
  foul,
  matchEnd,
} = require("./eventFactory");

async function runScenario() {
  const kickoffEvent = kickoff(matchId);
  await publishEvent(kickoffEvent);

  await sleep(3000);

  const goalEvent = goal(matchId, "home");
  await publishEvent(goalEvent);

  await sleep(2000);

  const varEvent = varCheck(matchId, "home", goalEvent.event_id);
  await publishEvent(varEvent);

  console.log(`[PRODUCER] Esperando ${varDelayMs}ms para resolver VAR...`);
  await sleep(varDelayMs);

  const annulledEvent = goalAnnulled(matchId, goalEvent.event_id);
  await publishEvent(annulledEvent);

  await sleep(3000);

  const foulEvent = foul(matchId, "away");
  await publishEvent(foulEvent);

  await sleep(3000);

  const endEvent = matchEnd(matchId);
  await publishEvent(endEvent);
}

module.exports = { runScenario };