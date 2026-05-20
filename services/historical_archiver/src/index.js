// ============================================================
// index.js — Punto de entrada del historical_archiver
// ============================================================
// Archivo mínimo que arranca el servicio. Delega toda la lógica
// a archiverService.js para mantener este entry point limpio.
// El proceso de Node.js queda corriendo indefinidamente porque
// el consumer de Kafka mantiene la conexión abierta y escucha
// mensajes hasta que el contenedor se detenga.
// ============================================================

// Importamos la función que contiene toda la lógica del archivador
const { startArchiver } = require("./archiverService");

// ------------------------------------------------------------
// bootstrap()
// Función envoltorio que maneja errores de arranque.
// Si startArchiver() falla (ej: Kafka no disponible), el proceso
// termina con código 1 para que Docker lo reinicie automáticamente.
// ------------------------------------------------------------
async function bootstrap() {
  try {
    // Inicia el consumer de Kafka y queda escuchando mensajes
    await startArchiver();
  } catch (error) {
    // Si hay un error crítico al arrancar, lo registramos y
    // terminamos el proceso para que el restart policy de Docker
    // vuelva a intentar levantar el servicio
    console.error("[ARCHIVER] Error:", error);
    process.exit(1); // Código 1 = error, Docker intentará reiniciar
  }
}

// Llamamos a bootstrap() al ejecutar este archivo
bootstrap();
