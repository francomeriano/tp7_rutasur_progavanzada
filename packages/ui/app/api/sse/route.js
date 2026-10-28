import mongoose from 'mongoose';

const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/rutasur?replicaSet=rs0&directConnection=true';

async function getDb() {
  if (mongoose.connection.readyState !== 1) {
    await mongoose.connect(MONGO_URI);
  }
  return mongoose.connection.db;
}

export async function GET() {
  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    async start(controller) {
      try {
        const db = await getDb();
        const collection = db.collection('envios');

        // Enviar datos iniciales guardados en Mongo
        const initialDocs = await collection.find({}).toArray();
        controller.enqueue(encoder.encode(`data: ${JSON.stringify({ type: 'INIT', data: initialDocs })}\n\n`));

        // Escuchar eventos en vivo mediante Change Stream (updateLookup)
        const changeStream = collection.watch([], { fullDocument: 'updateLookup' });

        changeStream.on('change', (change) => {
          if (change.fullDocument) {
            controller.enqueue(encoder.encode(`data: ${JSON.stringify({ type: 'UPDATE', data: change.fullDocument })}\n\n`));
          }
        });

      } catch (err) {
        console.error('Error en SSE Change Stream:', err);
      }
    }
  });

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive'
    }
  });
}
