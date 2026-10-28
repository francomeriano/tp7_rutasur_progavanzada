import { Kafka } from 'kafkajs';
import { v4 as uuidv4 } from 'uuid';

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';

const kafka = new Kafka({
  clientId: 'rescheduling-service',
  brokers: [KAFKA_BROKER]
});

const consumer = kafka.consumer({ groupId: 'reprogramacion' });
const producer = kafka.producer();

// Guardamos en memoria la cuenta de intentos por paquete
const intentosPorEnvio = new Map();

async function main() {
  await consumer.connect();
  await producer.connect();
  console.log('🚀 Servicio de Reprogramación conectado a Kafka');

  await consumer.subscribe({ topic: 'envios.estados', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const payload = JSON.parse(message.value.toString());

        if (payload.tipo === 'ENTREGA_FALLIDA') {
          const { shipmentId, motivoFallo, hubId, zonaId } = payload;
          
          const intentos = (intentosPorEnvio.get(shipmentId) || 0) + 1;
          intentosPorEnvio.set(shipmentId, intentos);

          let decision = '';
          let fechaNuevoIntento = null;

          // Regla: 3er intento fallido pasa directamente a devolución
          if (intentos >= 3) {
            decision = 'DEVOLUCION_COMERCIO';
          } else {
            switch (motivoFallo) {
              case 'AUSENTE':
                decision = 'REPROGRAMADO';
                // Mañana a la misma hora
                fechaNuevoIntento = new Date(Date.now() + 24 * 3600000).toISOString();
                break;
              case 'DIRECCION_INCORRECTA':
                decision = 'RETENIDO';
                break;
              case 'RECHAZADO':
                decision = 'DEVOLUCION_COMERCIO';
                break;
              default:
                decision = 'REPROGRAMADO';
                fechaNuevoIntento = new Date(Date.now() + 24 * 3600000).toISOString();
            }
          }

          const reprogramacionEvent = {
            eventId: uuidv4(),
            shipmentId,
            decision,
            motivoOriginal: motivoFallo,
            intentoNumero: intentos,
            fechaNuevoIntento,
            hubId,
            zonaId,
            procesadoEn: new Date().toISOString(),
            version: '1.0'
          };

          await producer.send({
            topic: 'envios.reprogramaciones',
            messages: [{
              key: shipmentId,
              value: JSON.stringify(reprogramacionEvent)
            }]
          });

          console.log(`[Reprogramación] Decisión para ${shipmentId} (Intento #${intentos}): ${decision}`);
        }
      } catch (err) {
        console.error('[Reprogramación] Error al procesar evento:', err.message);
      }
    }
  });
}

main().catch(console.error);