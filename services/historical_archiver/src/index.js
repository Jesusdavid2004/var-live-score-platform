// =============================================================================
// historical_archiver/src/index.js — Punto de entrada del archivador histórico
// =============================================================================
// Este archivo es el punto de entrada minimalista del historical_archiver.
// Sigue el principio de "thin entry point": este archivo solo se encarga del
// arranque y del manejo de errores críticos. Toda la lógica real está en
// archiverService.js para mantener este archivo limpio y fácil de leer.
//
// ¿Qué hace el historical_archiver en el sistema?
// Es el "registrador oficial" de todos los eventos del partido. Consume
// TODOS los eventos de Kafka y los persiste para auditoría y análisis histórico.
// Es el único servicio que nunca se "salta" ningún evento: su groupId propio
// garantiza que Kafka le entregue una copia de cada mensaje sin excepción.
//
// En este ejercicio: persiste en consola (para demostración).
// En producción: escribiría en PostgreSQL, MongoDB, S3 o Elasticsearch.
//
// El proceso Node.js NO termina al completar bootstrap() porque el consumer
// de Kafka mantiene una conexión TCP abierta que espera mensajes indefinidamente.
// =============================================================================

// Importamos la función principal del archivador.
// Toda la lógica de conexión a Kafka y procesamiento de mensajes está ahí.
const { startArchiver } = require("./archiverService");

// =============================================================================
// FUNCIÓN ASYNC: bootstrap()
// =============================================================================
// Función envoltorio que inicia el archivador y maneja errores de arranque.
//
// ¿Por qué una función bootstrap() en lugar de llamar startArchiver() directamente?
// Al envolver la llamada en una función async, podemos usar try/catch para
// capturar errores de arranque (ej: Kafka no disponible) y terminar el proceso
// con código 1, que hace que Docker lo reinicie automáticamente.
//
// Sin el try/catch, un error en startArchiver() generaría una UnhandledPromiseRejection
// que también terminaría el proceso, pero con un mensaje de error menos claro.
// =============================================================================
async function bootstrap() {
  try {
    // Inicia la conexión a Kafka y el loop de consumo de mensajes.
    // Esta llamada "bloquea" el proceso (no termina) porque el consumer
    // de KafkaJS mantiene la conexión abierta esperando nuevos eventos.
    await startArchiver();
  } catch (error) {
    // Error crítico al arrancar (ej: Kafka no disponible, topic no existe):
    // lo registramos en consola para que aparezca en los logs de Docker
    console.error("[ARCHIVER] Error:", error);

    // Terminamos con código 1: señal estándar de error en Unix/Linux.
    // Docker detecta este código y aplica la restart policy del servicio
    // (definida en docker-compose.yml), reintentando el arranque automáticamente.
    process.exit(1);
  }
}

// Llamamos a bootstrap() inmediatamente al ejecutar este archivo.
// Node.js ejecuta el módulo de arriba a abajo y al llegar aquí inicia el archivador.
bootstrap();
