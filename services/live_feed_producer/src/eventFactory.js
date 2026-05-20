// ============================================================
// eventFactory.js — Fábrica de eventos de partido
// ============================================================
// Este módulo implementa el patrón "Factory": centraliza la
// creación de todos los tipos de eventos que el productor puede
// publicar. Así el resto del código no necesita construir los
// objetos JSON a mano, lo que evita errores de tipeo y garantiza
// que todos los eventos tengan la misma estructura base.
//
// Cada función recibe los datos específicos del evento y retorna
// un objeto JSON listo para publicar en Kafka.
// ============================================================

// Importamos las utilidades para timestamps e IDs únicos
const { nowIso, makeEventId } = require("./utils");

// ------------------------------------------------------------
// createBaseEvent(matchId, eventType, eventPrefix)
// Construye el esqueleto común a TODOS los eventos.
// Todos los eventos del sistema tienen: match_id, event_type,
// event_id y timestamp. Al centralizar esto en una sola función,
// si el contrato cambia solo hay que modificar un lugar.
// ------------------------------------------------------------
function createBaseEvent(matchId, eventType, eventPrefix) {
  return {
    match_id: matchId,       // ID del partido al que pertenece el evento
    event_type: eventType,   // Tipo de evento (GOAL, VAR_CHECK, etc.)
    event_id: makeEventId(eventPrefix), // ID único con prefijo legible
    timestamp: nowIso(),     // Momento exacto en que ocurre el evento
  };
}

// ------------------------------------------------------------
// kickoff(matchId)
// Señal de inicio del partido. Es el primer evento que publica
// el productor. Los consumidores lo usan para inicializar el
// marcador en 0-0 y preparar su estado interno.
// ------------------------------------------------------------
function kickoff(matchId) {
  return createBaseEvent(matchId, "KICKOFF", "evt_k");
}

// ------------------------------------------------------------
// goal(matchId, team)
// Evento de gol. Incluye el campo "team" que indica si marcó
// el equipo local ("home") o visitante ("away").
// El match_state_service usa este campo para incrementar el
// contador correcto en el marcador.
// ------------------------------------------------------------
function goal(matchId, team) {
  return {
    ...createBaseEvent(matchId, "GOAL", "evt_g"),
    team, // "home" o "away"
  };
}

// ------------------------------------------------------------
// varCheck(matchId, team, relatedEventId)
// Indica que el VAR está revisando una jugada.
// El campo "related_event_id" apunta al event_id del gol que
// se está revisando, lo que permite al match_state_service
// encontrar ese gol en su registro y anularlo si corresponde.
// ------------------------------------------------------------
function varCheck(matchId, team, relatedEventId) {
  return {
    ...createBaseEvent(matchId, "VAR_CHECK", "evt_v"),
    team,
    related_event_id: relatedEventId, // ID del gol bajo revisión VAR
  };
}

// ------------------------------------------------------------
// goalAnnulled(matchId, annulsEventId)
// Confirma que el VAR anuló el gol. El campo "annuls_event_id"
// contiene el event_id del gol original, así el match_state_service
// puede descontar exactamente ese gol del marcador.
// Sin este ID la anulación sería ambigua si hay varios goles.
// ------------------------------------------------------------
function goalAnnulled(matchId, annulsEventId) {
  return {
    ...createBaseEvent(matchId, "GOAL_ANNULLED", "evt_ga"),
    annuls_event_id: annulsEventId, // El gol que se revierte
  };
}

// ------------------------------------------------------------
// foul(matchId, team)
// Evento de falta. No modifica el marcador pero se propaga a
// todos los consumidores para que puedan registrarlo.
// El betting_suspension_service no actúa sobre faltas.
// ------------------------------------------------------------
function foul(matchId, team) {
  return {
    ...createBaseEvent(matchId, "FOUL", "evt_f"),
    team, // equipo que cometió la falta
  };
}

// ------------------------------------------------------------
// matchEnd(matchId)
// Señal de fin del partido. Indica a todos los consumidores
// que no llegará ningún evento más para este match_id.
// El betting_suspension_service lo usa para reanudar apuestas.
// ------------------------------------------------------------
function matchEnd(matchId) {
  return createBaseEvent(matchId, "MATCH_END", "evt_m");
}

// Exportamos todas las funciones para que scenario.js las use
module.exports = {
  kickoff,
  goal,
  varCheck,
  goalAnnulled,
  foul,
  matchEnd,
};
