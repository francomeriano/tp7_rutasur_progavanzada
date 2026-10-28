import { Kafka } from 'kafkajs';
import { v4 as uuidv4 } from 'uuid';

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';

const kafka = new Kafka({
  clientId: 'notifications-service',
  brokers: [KAFKA_BROKER]
});

const consumer = kafka.consumer({ groupId: 'notificaciones' });
const producer = kafka.producer();

async function main() {
  await consumer.connect();
  await producer.connect();
  console.log('🚀 Servicio de Notificaciones conectado a Kafka');

  await consumer.subscribe({ topic: 'envios.estados', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const payload = JSON.parse(message.value.toString());

        // Reacciona únicamente ante entregas fallidas o alertas SLA
        if (payload.tipo === 'ENTREGA_FALLIDA') {
          const notificationEvent = {
            eventId: uuidv4(),
            shipmentId: payload.shipmentId,
            tipoNotificacion: 'AVISO_ENTREGA_FALLIDA',
            motivo: payload.motivoFallo,
            mensaje: `No pudimos entregar tu paquete (${payload.shipmentId}). Motivo: ${payload.motivoFallo}.`,
            enviadoEn: new Date().toISOString(),
            version: '1.0'
          };

          await producer.send({
            topic: 'notificaciones.enviadas',
            messages: [{
              key: payload.shipmentId,
              value: JSON.stringify(notificationEvent)
            }]
          });

          console.log(`[Notificaciones] Aviso generado para ${payload.shipmentId} (${payload.motivoFallo})`);
        }
      } catch (err) {
        console.error('[Notificaciones] Error al procesar evento:', err.message);
      }
    }
  });
}

main().catch(console.error);