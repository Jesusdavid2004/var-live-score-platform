// ============================================================
// utils.js — Funciones utilitarias reutilizables
// ============================================================
// Agrupa pequeñas funciones de propósito general que usan
// varios módulos del productor. Al tenerlas separadas se evita
// duplicar código y se facilita el testing unitario de cada
// función de forma aislada.
// ============================================================

// Módulo nativo de Node.js para generar identificadores únicos
// universales (UUID v4). No requiere dependencia externa.
const { randomUUID } = require("crypto");

// ------------------------------------------------------------
// nowIso()
// Devuelve la fecha y hora actual en formato ISO 8601.
// Ejemplo: "2025-05-19T15:30:00.000Z"
// Se usa en cada evento para registrar exactamente cuándo
// ocurrió, lo que permite ordenarlos cronológicamente
// y auditarlos después.
// ------------------------------------------------------------
function nowIso() {
  return new Date().toISOString();
}

// ------------------------------------------------------------
// sleep(ms)
// Pausa la ejecución del código asíncrono durante `ms`
// milisegundos. Devuelve una Promise que se resuelve después
// del tiempo indicado.
//
// ¿Por qué se necesita?
// El escenario de partido tiene pausas entre eventos (ej: 3 s
// entre el kickoff y el gol). JavaScript es de un solo hilo,
// así que se usa async/await + setTimeout para simular tiempo
// real sin bloquear el proceso.
// ------------------------------------------------------------
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ------------------------------------------------------------
// makeEventId(prefix)
// Genera un ID único para cada evento combinando un prefijo
// legible (ej: "evt_g") con los primeros 8 caracteres de un
// UUID aleatorio.
// Ejemplo de resultado: "evt_g_a3f2b60b"
//
// ¿Por qué un prefijo?
// Facilita identificar el tipo de evento con solo ver el ID,
// útil para depurar los logs sin necesidad de leer el JSON
// completo del mensaje.
// ------------------------------------------------------------
function makeEventId(prefix) {
  return `${prefix}_${randomUUID().slice(0, 8)}`;
}

// Exportamos las tres funciones para que otros módulos las importen
module.exports = { nowIso, sleep, makeEventId };
