// ============================================================
// index.js — Punto de entrada del live_feed_producer
// ============================================================
// Este es el archivo que Node.js ejecuta al arrancar el servicio.
// Su única responsabilidad es orquestar el ciclo de vida del
// productor: conectar a Kafka, correr el escenario y desconectar.
//
// ¿Por qué separar la lógica en otros módulos?
// Si todo estuviera aquí el archivo crecería indefinidamente.
// Separar cada responsabilidad (config, kafka, events, scenario)
// hace el código más mantenible y fácil de explicar.
// ============================================================

// Importamos el productor Kafka ya instanciado (no lo creamos aquí)
const { producer } = require("./kafka");

// Importamos el escenario del partido que define la secuencia de eventos
const { runScenario } = require("./scenario");

// ------------------------------------------------------------
// start()
// Función principal asíncrona que controla todo el ciclo de vida
// del productor.
//
// Estructura try/catch/finally:
//   - try:     conecta y ejecuta el escenario completo
//   - catch:   registra cualquier error inesperado
//   - finally: SIEMPRE desconecta el productor, haya error o no.
//              Esto es importante para no dejar conexiones abiertas
//              al broker de Kafka cuando el contenedor se detenga.
// ------------------------------------------------------------
async function start() {
  try {
    // Establece la conexión TCP con el broker de Kafka.
    // Si Kafka no está disponible, lanzará una excepción aquí.
    await producer.connect();
    console.log("[PRODUCER] Conectado a Kafka");

    // Ejecuta la secuencia completa del partido.
    // Esta función es asíncrona y dura el tiempo del escenario
    // (kickoff + gol + VAR + anulación + foul + fin).
    await runScenario();

  } catch (error) {
    // Si hay algún error en la conexión o en el escenario,
    // lo registramos en consola para facilitar el diagnóstico
    console.error("[PRODUCER] Error:", error);

  } finally {
    // La cláusula finally se ejecuta SIEMPRE, incluso si hubo error.
    // Desconectamos limpiamente para liberar los recursos de red.
    await producer.disconnect();
    console.log("[PRODUCER] Desconectado de Kafka");
  }
}

// Llamamos a start() inmediatamente al ejecutar este archivo.
// El proceso de Node.js terminará cuando start() se resuelva,
// lo que hace que el contenedor Docker se detenga (exit code 0).
start();
