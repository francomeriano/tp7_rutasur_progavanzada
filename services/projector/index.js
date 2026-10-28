import { Kafka } from 'kafkajs';
import mongoose from 'mongoose';

const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/rutasur?replicaSet=rs0&directConnection=true';
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');

async function run() {
  try {
    await mongoose.connect(MONGO_URI);
    console.log('✅ Proyector conectado a MongoDB');

    const kafka = new Kafka({
      clientId: 'rutasur-projector',
      brokers: KAFKA_BROKERS
    });

    const consumer = kafka.consumer({ groupId: `projector-group-${Date.now()}` });

    await consumer.connect();
    console.log('🚀 Proyector conectado a Kafka');

    await consumer.subscribe({ topic: 'envios.estados', fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const payload = JSON.parse(message.value.toString());
        console.log(`📥 [Proyector] Evento recibido: ${payload.estado || payload.tipo} para ${payload.shipmentId}`);

        const collection = mongoose.connection.db.collection('envios');

        await collection.updateOne(
          { shipmentId: payload.shipmentId },
          {
            $set: {
              shipmentId: payload.shipmentId,
              estadoActual: payload.estado || payload.tipo,
              hubId: payload.hubId || 'HUB-CENTRAL',
              zonaId: payload.zonaId || 'ZONA-URBANA',
              ultimaSecuencia: message.offset,
              updatedAt: new Date()
            }
          },
          { upsert: true }
        );

        console.log(`💾 Guardado en MongoDB: ${payload.shipmentId}`);
      }
    });
  } catch (err) {
    console.error('❌ Error en Proyector:', err);
  }
}

run();
