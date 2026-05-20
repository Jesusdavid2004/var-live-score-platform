// ============================================================
// suspensionLogic.js — Reglas de negocio para suspensión de apuestas
// ============================================================
// Este módulo contiene la lógica pura de decisión: dado un evento
// del partido, determina si se deben suspender o reanudar las
// apuestas, o si no hace falta hacer nada.
//
// ¿Por qué suspender apuestas?
// En las casas de apuestas deportivas reales, cuando ocurre un
// gol o entra el VAR, se suspenden las apuestas en vivo para
// evitar que alguien apueste con información privilegiada antes
// de que el sistema actualice las cuotas. Este servicio simula
// exactamente esa lógica.
//
// Reglas de negocio:
//   SUSPENDER cuando: GOAL, VAR_CHECK (incertidumbre en el partido)
//   REANUDAR cuando:  GOAL_ANNULLED, KICKOFF, MATCH_END (el partido está estable)
//   IGNORAR cuando:   FOUL y cualquier otro evento
// ============================================================

// ------------------------------------------------------------
// evaluateBettingAction(event)
// Evalúa el tipo de evento y decide qué comando de apuestas emitir.
//
// Retorna un objeto comando si corresponde actuar, o null si
// el evento no requiere ninguna acción sobre las apuestas.
//
// Formato del comando retornado:
//   { command: "SUSPEND_BETS" | "RESUME_BETS", match_id, reason }
// ------------------------------------------------------------
function evaluateBettingAction(event) {
  const { event_type, match_id } = event;

  // Tipos de evento que requieren SUSPENDER las apuestas:
  // - GOAL: puede haber revisión VAR, las cuotas quedan desactualizadas
  // - VAR_CHECK: el árbitro está revisando, resultado incierto
  const suspendTypes = ["GOAL", "VAR_CHECK"];

  // Tipos de evento que permiten REANUDAR las apuestas:
  // - GOAL_ANNULLED: el VAR terminó, el marcador está estabilizado
  // - KICKOFF: inicio del partido, todo en orden
  // - MATCH_END: fin del partido, se cierran todas las apuestas
  const resumeTypes = ["GOAL_ANNULLED", "KICKOFF", "MATCH_END"];

  if (suspendTypes.includes(event_type)) {
    // Emitimos comando de suspensión con el motivo del evento
    return {
      command: "SUSPEND_BETS",
      match_id,
      reason: event_type, // ej: "GOAL" o "VAR_CHECK"
    };
  }

  if (resumeTypes.includes(event_type)) {
    // Emitimos comando de reanudación con el motivo del evento
    return {
      command: "RESUME_BETS",
      match_id,
      reason: event_type, // ej: "GOAL_ANNULLED" o "MATCH_END"
    };
  }

  // Para FOUL y cualquier evento no listado, no hacemos nada
  return null;
}

module.exports = { evaluateBettingAction };
