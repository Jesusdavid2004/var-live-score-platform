function evaluateBettingAction(event) {
  const { event_type, match_id } = event;

  const suspendTypes = ["GOAL", "VAR_CHECK"];
  const resumeTypes = ["GOAL_ANNULLED", "KICKOFF", "MATCH_END"];

  if (suspendTypes.includes(event_type)) {
    return {
      command: "SUSPEND_BETS",
      match_id,
      reason: event_type,
    };
  }

  if (resumeTypes.includes(event_type)) {
    return {
      command: "RESUME_BETS",
      match_id,
      reason: event_type,
    };
  }

  return null;
}

module.exports = { evaluateBettingAction };