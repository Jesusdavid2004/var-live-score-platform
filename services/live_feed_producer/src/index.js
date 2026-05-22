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

// sleep: necesario para la pausa entre repeticiones del partido
const { sleep } = require("./utils");

// Segundos de espera entre el fin de un partido y el inicio del siguiente
const PAUSA_ENTRE_PARTIDOS_MS = 10000;

// ------------------------------------------------------------
// start()
// Función principal asíncrona que controla todo el ciclo de vida
// del productor.
//
// Ejecuta el escenario completo en un bucle infinito:
//   1. Conecta a Kafka (una sola vez)
//   2. Corre el partido completo (runScenario)
//   3. Espera PAUSA_ENTRE_PARTIDOS_MS milisegundos
//   4. Vuelve al paso 2 indefinidamente
//
// El contenedor nunca termina por sí solo; solo se detiene si
// Docker lo para explícitamente o si ocurre un error fatal.
// ------------------------------------------------------------
async function start() {
  try {
    // Establece la conexión TCP con el broker de Kafka.
    // Si Kafka no está disponible, lanzará una excepción aquí.
    await producer.connect();
    console.log("[PRODUCER] Conectado a Kafka");

    // Bucle infinito: el partido se repite automáticamente
    while (true) {
      console.log("[PRODUCER] ── Iniciando nuevo partido ──");
      await runScenario();
      console.log(`[PRODUCER] Partido finalizado. Reiniciando en ${PAUSA_ENTRE_PARTIDOS_MS / 1000}s...`);
      await sleep(PAUSA_ENTRE_PARTIDOS_MS);
    }

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
start();
