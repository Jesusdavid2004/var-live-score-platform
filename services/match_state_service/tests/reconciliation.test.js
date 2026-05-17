const { processEvent, getState, getGoalRegistry, reset } = require("../src/stateManager");

let passed = 0;
let failed = 0;

function assert(condition, testName) {
  if (condition) {
    console.log(`  PASS: ${testName}`);
    passed++;
  } else {
    console.error(`  FAIL: ${testName}`);
    failed++;
  }
}

function runTests() {
  console.log("\n=== T2.7 Prueba de Reconciliacion VAR ===\n");

  reset();

  console.log("-- Test 1: KICKOFF inicializa marcador --");
  processEvent({ match_id: "123", event_type: "KICKOFF", event_id: "evt_k001" });
  const s1 = getState("123");
  assert(s1.home === 0, "home === 0 despues de KICKOFF");
  assert(s1.away === 0, "away === 0 despues de KICKOFF");

  console.log("\n-- Test 2: GOAL incrementa marcador --");
  processEvent({ match_id: "123", event_type: "GOAL", event_id: "evt_g001", team: "home" });
  const s2 = getState("123");
  assert(s2.home === 1, "home === 1 despues de GOAL");
  assert(s2.away === 0, "away === 0 despues de GOAL");

  console.log("\n-- Test 3: GOAL_ANNULLED revierte el gol (1-0 -> 0-0) --");
  processEvent({
    match_id: "123",
    event_type: "GOAL_ANNULLED",
    event_id: "evt_ga001",
    annuls_event_id: "evt_g001",
  });
  const s3 = getState("123");
  assert(s3.home === 0, "home === 0 despues de GOAL_ANNULLED");
  assert(s3.away === 0, "away === 0 despues de GOAL_ANNULLED");

  console.log("\n-- Test 4: Multiples goles y anulacion selectiva --");
  reset();
  processEvent({ match_id: "456", event_type: "KICKOFF", event_id: "evt_k002" });
  processEvent({ match_id: "456", event_type: "GOAL", event_id: "evt_g010", team: "home" });
  processEvent({ match_id: "456", event_type: "GOAL", event_id: "evt_g011", team: "away" });
  processEvent({ match_id: "456", event_type: "GOAL", event_id: "evt_g012", team: "home" });
  const s4 = getState("456");
  assert(s4.home === 2, "home === 2 (dos goles locales)");
  assert(s4.away === 1, "away === 1 (un gol visitante)");

  processEvent({
    match_id: "456",
    event_type: "GOAL_ANNULLED",
    event_id: "evt_ga010",
    annuls_event_id: "evt_g010",
  });
  const s5 = getState("456");
  assert(s5.home === 1, "home === 1 despues de anular primer gol local");
  assert(s5.away === 1, "away === 1 (sin cambios)");

  console.log("\n-- Test 5: goalRegistry refleja goles activos --");
  const registry = getGoalRegistry();
  assert(registry.has("evt_g011"), "evt_g011 sigue en goalRegistry");
  assert(registry.has("evt_g012"), "evt_g012 sigue en goalRegistry");
  assert(!registry.has("evt_g010"), "evt_g010 fue eliminado del goalRegistry");

  console.log("\n-- Test 6: Suspencion de apuestas (logica de betting) --");
  const { evaluateBettingAction } = require("../../betting_suspension_service/src/suspensionLogic");

  const r1 = evaluateBettingAction({ event_type: "GOAL", match_id: "123" });
  assert(r1.command === "SUSPEND_BETS", "GOAL -> SUSPEND_BETS");

  const r2 = evaluateBettingAction({ event_type: "VAR_CHECK", match_id: "123" });
  assert(r2.command === "SUSPEND_BETS", "VAR_CHECK -> SUSPEND_BETS");

  const r3 = evaluateBettingAction({ event_type: "GOAL_ANNULLED", match_id: "123" });
  assert(r3.command === "RESUME_BETS", "GOAL_ANNULLED -> RESUME_BETS");

  const r4 = evaluateBettingAction({ event_type: "MATCH_END", match_id: "123" });
  assert(r4.command === "RESUME_BETS", "MATCH_END -> RESUME_BETS");

  const r5 = evaluateBettingAction({ event_type: "KICKOFF", match_id: "123" });
  assert(r5.command === "RESUME_BETS", "KICKOFF -> RESUME_BETS");

  const r6 = evaluateBettingAction({ event_type: "FOUL", match_id: "123" });
  assert(r6 === null, "FOUL -> null (no afecta apuestas)");

  console.log("\n===============================");
  console.log(`Resultados: ${passed} pasados, ${failed} fallidos`);
  if (failed > 0) {
    process.exit(1);
  }
}

runTests();