// ============================================================
// publisher.js — Publicación de eventos a Kafka
// ============================================================
// Responsabilidad única: tomar un objeto de evento y enviarlo
// al topic de Kafka en el formato correcto. Al separar esta
// lógica del escenario y del entry point, el código es más
// limpio y el envío a Kafka se puede cambiar (o mockear en
// tests) sin tocar el resto del sistema.
// ============================================================

// Importamos el productor ya instanciado (singleton de kafka.js)
const { producer } = require("./kafka");

// Importamos el nombre del topic desde la config centralizada
const { kafkaTopic } = require("./config");

// ------------------------------------------------------------
// publishEvent(event)
// Serializa el objeto de evento a JSON y lo envía a Kafka.
//
// Formato del mensaje Kafka:
//   - topic: el topic de destino (match_events)
//   - key: el match_id del evento (string)
//   - value: el evento serializado como JSON string
//
// ¿Por qué usar el match_id como key?
// Kafka garantiza que todos los mensajes con la misma key van
// a la misma partición, en orden. Esto asegura que los eventos
// del partido 123 siempre se procesen en orden por los consumidores,
// independientemente de cuántas particiones tenga el topic.
// ------------------------------------------------------------
async function publishEvent(event) {
  await producer.send({
    topic: kafkaTopic,
    messages: [
      {
        key: event.match_id,         // Clave de particionamiento (el partido)
        value: JSON.stringify(event), // El evento serializado como string JSON
      },
    ],
  });

  // Log de confirmación para verificar en consola que el envío fue exitoso
  console.log(`[PRODUCER] Evento enviado: ${event.event_type}`, event);
}

// Exportamos la función para que scenario.js la llame en cada paso
module.exports = { publishEvent };
