// =============================================================================
// betting_suspension_service/src/suspensionLogic.js — Reglas de apuestas
// =============================================================================
// Este módulo contiene la LÓGICA PURA de decisión sobre las apuestas en vivo.
// "Lógica pura" significa que solo trabaja con datos: recibe un evento y
// devuelve una decisión, sin hacer I/O (sin leer/escribir en red o disco).
//
// ¿Por qué separar la lógica en su propio módulo?
// Al aislar la lógica de negocio en un módulo puro (sin efectos secundarios),
// podemos probarla con tests unitarios simples sin necesitar Kafka ni RabbitMQ:
//   evaluateBettingAction({ event_type: "GOAL" }) → { command: "SUSPEND_BETS" }
//   evaluateBettingAction({ event_type: "FOUL" }) → null
//
// ¿Por qué suspender apuestas en eventos de fútbol?
// En las casas de apuestas reales, cuando ocurre un gol o entra el VAR:
//   1. Las cuotas (odds) de los mercados quedan desactualizadas inmediatamente
//   2. Si alguien ya vio el gol en TV pero el sistema aún no lo procesó,
//      podría apostar con información privilegiada ("gubbing")
//   3. La solución: suspender las apuestas hasta que el sistema recalcule cuotas
//
// Reglas de negocio implementadas:
//   SUSPENDER cuando: GOAL (posible VAR), VAR_CHECK (resultado incierto)
//   REANUDAR cuando:  GOAL_ANNULLED (VAR terminó), KICKOFF, MATCH_END
//   IGNORAR cuando:   FOUL y cualquier otro evento no listado
// =============================================================================

// =============================================================================
// FUNCIÓN: evaluateBettingAction(event)
// =============================================================================
// Evalúa el tipo de evento del partido y decide qué acción de apuestas tomar.
//
// Parámetro:
//   event → objeto con al menos { event_type, match_id }
//
// Retorna:
//   { command, match_id, reason } si se requiere acción (suspender o reanudar)
//   null si el evento no requiere ninguna acción sobre las apuestas
//
// Formato del comando retornado:
//   {
//     command: "SUSPEND_BETS" | "RESUME_BETS",
//     match_id: "123",    // ID del partido afectado
//     reason: "GOAL"      // Tipo de evento que causó la acción (para logs/auditoría)
//   }
// =============================================================================
function evaluateBettingAction(event) {
  const { event_type, match_id } = event; // Extraemos solo los campos necesarios

  // ── Eventos que requieren SUSPENDER las apuestas ────────────────────────────
  // GOAL: puede ser anulado por el VAR, las cuotas del resultado son inciertas.
  //       Las apuestas se suspenden hasta saber si el gol es válido.
  // VAR_CHECK: el árbitro de video está revisando, el resultado es desconocido.
  //            Nadie debería poder apostar mientras el VAR no haya decidido.
  const suspendTypes = ["GOAL", "VAR_CHECK"];

  // ── Eventos que permiten REANUDAR las apuestas ──────────────────────────────
  // GOAL_ANNULLED: el VAR terminó su revisión y anuló el gol. El marcador
  //                está estabilizado, se pueden recalcular cuotas y reanudar.
  // KICKOFF: inicio del partido, todo está en orden para apostar.
  // MATCH_END: fin del partido, se cierran todos los mercados abiertos.
  const resumeTypes = ["GOAL_ANNULLED", "KICKOFF", "MATCH_END"];

  if (suspendTypes.includes(event_type)) {
    // Retornamos el comando de suspensión con el motivo (para trazabilidad/auditoría)
    return {
      command: "SUSPEND_BETS", // Acción: suspender todos los mercados del partido
      match_id,                // ID del partido para que el worker sepa qué mercado afectar
      reason: event_type,      // Motivo: "GOAL" o "VAR_CHECK" (para logs y debugging)
    };
  }

  if (resumeTypes.includes(event_type)) {
    // Retornamos el comando de reanudación cuando el partido se estabiliza
    return {
      command: "RESUME_BETS", // Acción: reabrir los mercados del partido
      match_id,
      reason: event_type,     // Motivo: "GOAL_ANNULLED", "KICKOFF" o "MATCH_END"
    };
  }

  // Para FOUL y cualquier evento no listado: null indica "no hacer nada".
  // index.js verifica este null y no llama a publishBettingCommand() en ese caso.
  return null;
}

// Exportamos la función para que index.js la use en el loop de consumo de Kafka
module.exports = { evaluateBettingAction };
