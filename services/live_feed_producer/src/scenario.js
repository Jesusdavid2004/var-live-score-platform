// ============================================================
// scenario.js — Guión del partido simulado
// ============================================================
// Este módulo define la secuencia completa de eventos de un
// partido de fútbol con un flujo VAR incluido. Es la "historia"
// que el productor narra publicando eventos a Kafka.
//
// ¿Por qué simular un partido?
// En un sistema real este módulo sería reemplazado por una
// fuente de datos en vivo (API de la federación, chips GPS,
// sensores, etc.). Para el ejercicio académico simulamos el
// partido con tiempos fijos para que sea reproducible y
// demostrable en el aula.
//
// Secuencia del escenario:
//   1. KICKOFF          → inicio del partido
//   2. GOAL (home)      → gol del equipo local → 1-0
//   3. VAR_CHECK        → revisión del gol anterior
//   4. [espera VAR]     → ~20 segundos simulando la revisión
//   5. GOAL_ANNULLED    → el VAR anula el gol → 0-0
//   6. FOUL (away)      → falta del equipo visitante
//   7. MATCH_END        → fin del partido
// ============================================================

// Importamos la configuración para saber el matchId y varDelayMs
const { matchId, varDelayMs } = require("./config");

// sleep: para pausar entre eventos y simular el tiempo real del partido
const { sleep } = require("./utils");

// publishEvent: la función que envía cada evento a Kafka
const { publishEvent } = require("./publisher");

// Importamos todas las funciones de creación de eventos
const {
  kickoff,
  goal,
  varCheck,
  goalAnnulled,
  foul,
  matchEnd,
} = require("./eventFactory");

// ------------------------------------------------------------
// runScenario()
// Ejecuta el guión completo del partido de forma asíncrona.
// Cada evento se publica y luego se espera un tiempo antes
// del siguiente para simular los intervalos reales del partido.
// ------------------------------------------------------------
async function runScenario() {
  // Paso 1: Inicio del partido
  const kickoffEvent = kickoff(matchId);
  await publishEvent(kickoffEvent);
  await sleep(5000); // 5 segundos antes de la primera falta

  // Paso 2: Falta del equipo visitante (antes del gol)
  const foulEvent = foul(matchId, "away");
  await publishEvent(foulEvent);
  await sleep(5000); // 5 segundos antes del gol

  // Paso 3: Gol del equipo local
  // Guardamos el evento para referenciar su ID en el VAR_CHECK
  const goalEvent = goal(matchId, "home");
  await publishEvent(goalEvent);
  await sleep(5000); // 5 segundos antes de que el VAR intervenga

  // Paso 4: Revisión VAR del gol recién marcado
  // related_event_id apunta al gol para que los consumidores sepan
  // cuál gol está siendo revisado en este momento
  const varEvent = varCheck(matchId, "home", goalEvent.event_id);
  await publishEvent(varEvent);

  // Espera mientras el VAR revisa (configurable en .env, por defecto 20s)
  console.log(`[PRODUCER] Esperando ${varDelayMs}ms para resolver VAR...`);
  await sleep(varDelayMs);

  // Paso 5: El VAR decide anular el gol
  // annuls_event_id contiene el event_id del gol original para que
  // el match_state_service pueda revertir exactamente ese gol
  const annulledEvent = goalAnnulled(matchId, goalEvent.event_id);
  await publishEvent(annulledEvent);
  await sleep(5000); // 5 segundos hasta el pitido final

  // Paso 6: Fin del partido
  // Señal para que todos los consumidores sepan que no habrá más eventos
  const endEvent = matchEnd(matchId);
  await publishEvent(endEvent);
}

// Exportamos la función para que index.js la llame al iniciar
module.exports = { runScenario };
