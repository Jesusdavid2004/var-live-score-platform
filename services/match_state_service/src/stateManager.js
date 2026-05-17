const matchState = new Map();
const goalRegistry = new Map();

function initMatch(matchId) {
  if (!matchState.has(matchId)) {
    matchState.set(matchId, { home: 0, away: 0 });
  }
}

function processEvent(event) {
  const { match_id, event_type, event_id, team, annuls_event_id } = event;
  initMatch(match_id);

  const state = matchState.get(match_id);

  switch (event_type) {
    case "KICKOFF":
      console.log(`[STATE] Partido ${match_id} iniciado`);
      break;

    case "GOAL":
      if (team === "home" || team === "away") {
        state[team] += 1;
        goalRegistry.set(event_id, { match_id, team });
        console.log(
          `[STATE] GOL de ${team} | Marcador: ${state.home}-${state.away}`
        );
      }
      break;

    case "GOAL_ANNULLED": {
      const originalGoal = goalRegistry.get(annuls_event_id);
      if (originalGoal) {
        state[originalGoal.team] -= 1;
        goalRegistry.delete(annuls_event_id);
        console.log(
          `[STATE] GOL ANULADO de ${originalGoal.team} | Marcador: ${state.home}-${state.away}`
        );
      } else {
        console.warn(
          `[STATE] No se encontro gol original con event_id=${annuls_event_id}`
        );
      }
      break;
    }

    case "VAR_CHECK":
      console.log(`[STATE] VAR en revisión para ${team}`);
      break;

    case "FOUL":
      console.log(`[STATE] Falta de ${team}`);
      break;

    case "MATCH_END":
      console.log(
        `[STATE] Partido ${match_id} finalizado | Marcador: ${state.home}-${state.away}`
      );
      break;

    default:
      console.warn(`[STATE] Evento desconocido: ${event_type}`);
  }

  return { match_id, ...state, event_type };
}

function getState(matchId) {
  return matchState.get(matchId) || null;
}

function getGoalRegistry() {
  return goalRegistry;
}

function reset() {
  matchState.clear();
  goalRegistry.clear();
}

module.exports = { processEvent, getState, getGoalRegistry, reset };