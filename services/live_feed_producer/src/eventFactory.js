const { nowIso, makeEventId } = require("./utils");

function createBaseEvent(matchId, eventType, eventPrefix) {
  return {
    match_id: matchId,
    event_type: eventType,
    event_id: makeEventId(eventPrefix),
    timestamp: nowIso(),
  };
}

function kickoff(matchId) {
  return createBaseEvent(matchId, "KICKOFF", "evt_k");
}

function goal(matchId, team) {
  return {
    ...createBaseEvent(matchId, "GOAL", "evt_g"),
    team,
  };
}

function varCheck(matchId, team, relatedEventId) {
  return {
    ...createBaseEvent(matchId, "VAR_CHECK", "evt_v"),
    team,
    related_event_id: relatedEventId,
  };
}

function goalAnnulled(matchId, annulsEventId) {
  return {
    ...createBaseEvent(matchId, "GOAL_ANNULLED", "evt_ga"),
    annuls_event_id: annulsEventId,
  };
}

function foul(matchId, team) {
  return {
    ...createBaseEvent(matchId, "FOUL", "evt_f"),
    team,
  };
}

function matchEnd(matchId) {
  return createBaseEvent(matchId, "MATCH_END", "evt_m");
}

module.exports = {
  kickoff,
  goal,
  varCheck,
  goalAnnulled,
  foul,
  matchEnd,
};