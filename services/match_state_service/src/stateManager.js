// ============================================================
// stateManager.js — Motor de estado del marcador
// ============================================================
// Este módulo es el "cerebro" del match_state_service. Mantiene
// en memoria el marcador de cada partido y aplica la lógica de
// negocio para cada tipo de evento.
//
// ¿Por qué en memoria y no en base de datos?
// Para el ejercicio académico, la memoria es suficiente.
// En producción (ej: Premier League) se usaría Redis o PostgreSQL
// para que el estado sobreviva reinicios del servicio.
//
// Estructuras de datos utilizadas:
//   - matchState: Map<matchId, { home: number, away: number }>
//     Almacena el marcador actual de cada partido.
//   - goalRegistry: Map<eventId, { matchId, team }>
//     Registro de todos los goles anotados. Es crucial para poder
//     anular un gol específico cuando el VAR lo decide, porque
//     necesitamos saber de qué equipo fue el gol original.
// ============================================================

// Map que guarda el marcador de cada partido.
// Clave: match_id (string) | Valor: { home: 0, away: 0 }
const matchState = new Map();

// Map que registra cada gol anotado con su ID único.
// Clave: event_id del gol | Valor: { match_id, team }
// Se usa en GOAL_ANNULLED para revertir exactamente ese gol.
const goalRegistry = new Map();

// ------------------------------------------------------------
// initMatch(matchId)
// Inicializa el estado de un partido si aún no existe.
// Se llama al principio de processEvent() para garantizar que
// siempre haya un objeto de estado antes de modificarlo.
// ------------------------------------------------------------
function initMatch(matchId) {
  if (!matchState.has(matchId)) {
    // El partido empieza 0-0
    matchState.set(matchId, { home: 0, away: 0 });
  }
}

// ------------------------------------------------------------
// processEvent(event)
// Aplica la lógica de negocio del evento al estado del partido.
// Usa un switch para manejar cada tipo de evento por separado.
//
// Retorna el estado actualizado del partido (marcador + tipo de
// evento) para que index.js lo publique a RabbitMQ.
// Retorna null implícitamente si el evento no modifica el estado
// (KICKOFF, VAR_CHECK, FOUL no cambian el marcador pero igual
// retornan el estado para notificar al dashboard).
// ------------------------------------------------------------
function processEvent(event) {
  const { match_id, event_type, event_id, team, annuls_event_id } = event;

  // Aseguramos que el partido esté inicializado antes de cualquier operación
  initMatch(match_id);

  // Obtenemos la referencia al objeto de estado (es mutable directamente)
  const state = matchState.get(match_id);

  switch (event_type) {
    case "KICKOFF":
      // El inicio del partido solo registra el evento, no cambia el marcador.
      // El marcador ya se inicializó en 0-0 con initMatch().
      console.log(`[STATE] Partido ${match_id} iniciado`);
      break;

    case "GOAL":
      // Incrementamos el gol del equipo correspondiente y
      // registramos el gol en goalRegistry para poder anularlo después.
      if (team === "home" || team === "away") {
        state[team] += 1; // Incrementa home o away según corresponda
        goalRegistry.set(event_id, { match_id, team }); // Guardamos quién marcó
        console.log(
          `[STATE] GOL de ${team} | Marcador: ${state.home}-${state.away}`
        );
      }
      break;

    case "GOAL_ANNULLED": {
      // Buscamos el gol original en el registro usando su event_id.
      // Sin este registro no sabríamos qué equipo marcó ese gol.
      const originalGoal = goalRegistry.get(annuls_event_id);
      if (originalGoal) {
        state[originalGoal.team] -= 1; // Revertimos el gol del equipo que marcó
        goalRegistry.delete(annuls_event_id); // Limpiamos el registro
        console.log(
          `[STATE] GOL ANULADO de ${originalGoal.team} | Marcador: ${state.home}-${state.away}`
        );
      } else {
        // Si no encontramos el gol, lo registramos como advertencia.
        // Puede ocurrir si el servicio se reinició y perdió el goalRegistry.
        console.warn(
          `[STATE] No se encontro gol original con event_id=${annuls_event_id}`
        );
      }
      break;
    }

    case "VAR_CHECK":
      // El VAR CHECK no modifica el marcador. Solo lo registramos
      // para que el dashboard muestre el banner de revisión VAR.
      console.log(`[STATE] VAR en revisión para ${team}`);
      break;

    case "FOUL":
      // Las faltas tampoco modifican el marcador en este sistema.
      // En un sistema completo podrían disparar tarjetas amarillas/rojas.
      console.log(`[STATE] Falta de ${team}`);
      break;

    case "MATCH_END":
      // Fin del partido: solo registramos el resultado final.
      // En producción aquí se persistiría el resultado en BD.
      console.log(
        `[STATE] Partido ${match_id} finalizado | Marcador: ${state.home}-${state.away}`
      );
      break;

    default:
      // Evento desconocido: lo registramos para facilitar el debugging
      console.warn(`[STATE] Evento desconocido: ${event_type}`);
  }

  // Retornamos el estado actualizado junto con el tipo de evento.
  // index.js usa este objeto para publicarlo a RabbitMQ.
  return { match_id, ...state, event_type };
}

// ------------------------------------------------------------
// Funciones auxiliares (usadas principalmente en los tests)
// ------------------------------------------------------------

// Retorna el marcador actual de un partido, o null si no existe
function getState(matchId) {
  return matchState.get(matchId) || null;
}

// Retorna el Map completo de goles registrados (para debugging y tests)
function getGoalRegistry() {
  return goalRegistry;
}

// Limpia completamente el estado (usado en tests para reiniciar el módulo)
function reset() {
  matchState.clear();
  goalRegistry.clear();
}

module.exports = { processEvent, getState, getGoalRegistry, reset };
