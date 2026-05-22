// =============================================================================
// match_state_service/src/stateManager.js — Motor de estado del marcador
// =============================================================================
// Este módulo es el "cerebro" del match_state_service. Mantiene en memoria
// el marcador de cada partido y aplica la lógica de negocio para cada tipo
// de evento que llega desde Kafka.
//
// ¿Por qué "en memoria" y no en una base de datos?
// Para el ejercicio académico, mantener el estado en memoria (RAM) es
// suficiente y mucho más rápido. En un sistema real de producción (Premier
// League, Champions League) se usaría Redis o PostgreSQL para que el estado
// sobreviva reinicios del servicio y sea accesible desde múltiples instancias.
//
// Estructuras de datos utilizadas:
//   matchState:   Map<matchId, { home: number, away: number }>
//     → Guarda el marcador actual de cada partido.
//     → Clave: match_id (string "123"), Valor: objeto con goles de cada equipo.
//
//   goalRegistry: Map<eventId, { matchId, team }>
//     → Registro de TODOS los goles anotados con su ID único de evento.
//     → Es crucial para poder ANULAR un gol específico cuando el VAR lo decide,
//       porque necesitamos saber qué equipo marcó ese gol para restar el punto.
//     → Sin este registro, no podríamos distinguir cuál de varios goles anular.
// =============================================================================

// Map es una estructura clave-valor de JavaScript (como un diccionario/HashMap).
// A diferencia de los objetos ({}) regulares, Map permite cualquier tipo como clave
// y tiene mejor rendimiento para inserciones/búsquedas frecuentes.

// Marcador actual de cada partido: match_id → { home: 0, away: 0 }
const matchState = new Map();

// Registro de goles por event_id: event_id_del_gol → { match_id, team }
// Este mapa es la clave para implementar la anulación de goles del VAR
const goalRegistry = new Map();

// =============================================================================
// FUNCIÓN: initMatch(matchId)
// =============================================================================
// Inicializa el estado de un partido con marcador 0-0 si aún no existe.
//
// ¿Por qué verificar antes de inicializar?
// Si llegara un segundo KICKOFF (ej: por reenvío de mensajes en Kafka con
// fromBeginning: true), no queremos resetear el marcador a 0-0 si ya tiene
// goles. La verificación has() previene esa sobrescritura accidental.
//
// ¿Cuándo se llama?
// Al inicio de processEvent() para CUALQUIER tipo de evento, garantizando
// que siempre exista un estado antes de intentar modificarlo.
// =============================================================================
function initMatch(matchId) {
  if (!matchState.has(matchId)) {
    // El partido empieza con marcador 0-0 para ambos equipos
    matchState.set(matchId, { home: 0, away: 0 });
  }
  // Si el partido ya existe, no hacemos nada (preservamos el marcador actual)
}

// =============================================================================
// FUNCIÓN: processEvent(event)
// =============================================================================
// Aplica la lógica de negocio del evento al estado del partido en memoria.
// Es la función más importante del servicio: implementa las reglas del fútbol.
//
// Parámetro:
//   event → objeto con campos { match_id, event_type, event_id, team, annuls_event_id }
//
// Retorna:
//   { match_id, home, away, event_type } → estado actualizado para publicar
//   (retorno implícito de null si el switch no hace nada, pero en la práctica
//    siempre retorna el estado al final)
//
// Decisiones de diseño:
//   - Switch/case en lugar de if/else: más legible con múltiples tipos de evento
//   - Estado mutable directo: state.home++ es más simple que crear un nuevo objeto
//   - Spread operator al retornar: evita exponer la referencia interna del Map
// =============================================================================
function processEvent(event) {
  // Desestructuramos los campos que necesitamos del evento
  const { match_id, event_type, event_id, team, annuls_event_id } = event;

  // Nos aseguramos de que el partido esté inicializado ANTES de cualquier operación.
  // Si este es el primer evento de este match_id, crea el estado 0-0.
  initMatch(match_id);

  // Obtenemos la referencia directa al objeto de estado del partido.
  // Al ser una referencia (no una copia), modificar state.home modifica directamente
  // el valor guardado en el Map, sin necesidad de hacer set() de nuevo.
  const state = matchState.get(match_id);

  switch (event_type) {
    case "KICKOFF":
      // El inicio del partido no modifica el marcador (ya está en 0-0 por initMatch).
      // Solo registramos el evento para los logs de monitoreo.
      console.log(`[STATE] Partido ${match_id} iniciado`);
      break;

    case "GOAL":
      // Un gol válido incrementa el contador del equipo correspondiente
      // y registra el gol en goalRegistry para posible anulación posterior.
      if (team === "home" || team === "away") {
        state[team] += 1; // state["home"]++ o state["away"]++: incrementa el gol

        // Guardamos en goalRegistry: event_id → { match_id, team }
        // Este registro es FUNDAMENTAL: cuando llegue GOAL_ANNULLED con
        // annuls_event_id igual a este event_id, sabremos qué equipo marcó.
        goalRegistry.set(event_id, { match_id, team });

        console.log(
          `[STATE] GOL de ${team} | Marcador: ${state.home}-${state.away}`
        );
      }
      break;

    case "GOAL_ANNULLED": {
      // El VAR anuló un gol: debemos revertir exactamente ESE gol.
      // annuls_event_id es el event_id del gol original que se anula.
      // Sin el goalRegistry, no sabríamos de qué equipo fue el gol anulado.
      const originalGoal = goalRegistry.get(annuls_event_id);

      if (originalGoal) {
        state[originalGoal.team] -= 1; // Restamos el gol del equipo que había marcado
        goalRegistry.delete(annuls_event_id); // Limpiamos el registro del gol anulado

        console.log(
          `[STATE] GOL ANULADO de ${originalGoal.team} | Marcador: ${state.home}-${state.away}`
        );
      } else {
        // Si no encontramos el gol en el registro, puede ser porque:
        //   1. El servicio se reinició y perdió el goalRegistry en memoria
        //   2. El event_id del mensaje GOAL_ANNULLED está incorrecto
        console.warn(
          `[STATE] No se encontro gol original con event_id=${annuls_event_id}`
        );
      }
      break;
    }

    case "VAR_CHECK":
      // El VAR está revisando una jugada. No modifica el marcador.
      // Solo lo registramos para que el dashboard muestre el banner naranja.
      // (El dashboard recibe este evento vía RabbitMQ y activa el banner)
      console.log(`[STATE] VAR en revisión para ${team}`);
      break;

    case "FOUL":
      // Una falta no modifica el marcador en este sistema simplificado.
      // En un sistema completo podrían disparar tarjetas amarillas/rojas
      // y cambiar el estado de los jugadores.
      console.log(`[STATE] Falta de ${team}`);
      break;

    case "MATCH_END":
      // Fin del partido: registramos el resultado final.
      // En producción aquí se persistiría el resultado en base de datos
      // y se calcularían las estadísticas finales del partido.
      console.log(
        `[STATE] Partido ${match_id} finalizado | Marcador: ${state.home}-${state.away}`
      );
      break;

    default:
      // Evento desconocido: lo registramos pero no crasheamos.
      // Puede ocurrir si se agrega un nuevo tipo de evento al productor
      // sin actualizar el stateManager.
      console.warn(`[STATE] Evento desconocido: ${event_type}`);
  }

  // Retornamos el estado actualizado junto con el tipo de evento.
  // El spread operator (...state) crea una copia del objeto {home, away}
  // para evitar que el caller modifique accidentalmente el Map interno.
  // index.js usa este objeto para publicarlo a RabbitMQ.
  return { match_id, ...state, event_type };
}

// =============================================================================
// FUNCIONES AUXILIARES
// =============================================================================
// Estas funciones no son parte del flujo principal del servicio.
// Se usan principalmente en los tests unitarios para verificar el estado
// interno del módulo sin tener que publicar a Kafka/RabbitMQ.
// =============================================================================

// Retorna el marcador actual de un partido dado su ID.
// Retorna null si el partido no existe todavía en el Map.
// Útil en tests: "después de un GOAL, getState('123').home debe ser 1"
function getState(matchId) {
  return matchState.get(matchId) || null;
}

// Retorna el Map completo de goles registrados.
// Útil en tests para verificar que el goalRegistry se actualiza correctamente
// después de cada GOAL y se limpia después de cada GOAL_ANNULLED.
function getGoalRegistry() {
  return goalRegistry;
}

// Limpia completamente ambos Maps.
// Usado en tests para garantizar un estado limpio antes de cada test case.
// (Equivalente al "beforeEach" de la base de datos de un test de integración)
function reset() {
  matchState.clear();    // Borra todos los marcadores
  goalRegistry.clear();  // Borra todos los goles registrados
}

// Exportamos las funciones que otros módulos necesitan:
// - processEvent: usada por index.js en el loop de consumo de Kafka
// - getState, getGoalRegistry, reset: usadas por los tests unitarios
module.exports = { processEvent, getState, getGoalRegistry, reset };
