import { Kafka } from 'kafkajs';
import { v4 as uuidv4 } from 'uuid';

const kafka = new Kafka({
  clientId: 'simulator',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092']
});

const producer = kafka.producer();

const HUBS = ['HUB_CENTRO', 'HUB_NORTE', 'HUB_SUR'];
const ZONAS = ['ZONA_1', 'ZONA_2', 'ZONA_3'];

// 1. Escenario: Envío Feliz (Recorrido completo sin incidentes)
export async function runHappyPath(shipmentId = `ENV-HAPPY-${Date.now()}`) {
  const hubId = HUBS[0];
  const zonaId = ZONAS[0];
  let secuencia = 1;

  await producer.send({
    topic: 'envios.promesas',
    messages: [{
      key: shipmentId,
      value: JSON.stringify({
        eventId: uuidv4(),
        shipmentId,
        venceEn: new Date(Date.now() + 2 * 3600000).toISOString(),
        franjaHoraria: 'TARDE',
        hubId,
        zonaId,
        version: '1.0'
      })
    }]
  });

  const estados = ['CREADO', 'INGRESADO_HUB', 'CLASIFICADO', 'EN_REPARTO', 'ENTREGADO'];

  for (const estado of estados) {
    await producer.send({
      topic: 'envios.estados',
      messages: [{
        key: shipmentId,
        value: JSON.stringify({
          eventId: uuidv4(),
          shipmentId,
          tipo: estado,
          ocurridoEn: new Date().toISOString(),
          secuencia: secuencia++,
          hubId,
          zonaId,
          repartidorId: 'REP-01',
          version: '1.0'
        })
      }]
    });
    console.log(`[Simulador - HappyPath] ${shipmentId} -> ${estado}`);
    await new Promise(r => setTimeout(r, 1000));
  }
}

// 2. Escenario: Ausente (Entrega fallida por cliente ausente)
export async function runAbsentScenario(shipmentId = `ENV-ABSENT-${Date.now()}`) {
  const hubId = HUBS[1];
  const zonaId = ZONAS[1];
  let secuencia = 1;

  const estados = ['CREADO', 'INGRESADO_HUB', 'CLASIFICADO', 'EN_REPARTO'];

  for (const estado of estados) {
    await producer.send({
      topic: 'envios.estados',
      messages: [{
        key: shipmentId,
        value: JSON.stringify({
          eventId: uuidv4(),
          shipmentId,
          tipo: estado,
          ocurridoEn: new Date().toISOString(),
          secuencia: secuencia++,
          hubId,
          zonaId,
          repartidorId: 'REP-02',
          version: '1.0'
        })
      }]
    });
    await new Promise(r => setTimeout(r, 500));
  }

  // Emite ENTREGA_FALLIDA por AUSENTE
  await producer.send({
    topic: 'envios.estados',
    messages: [{
      key: shipmentId,
      value: JSON.stringify({
        eventId: uuidv4(),
        shipmentId,
        tipo: 'ENTREGA_FALLIDA',
        motivoFallo: 'AUSENTE',
        ocurridoEn: new Date().toISOString(),
        secuencia: secuencia++,
        hubId,
        zonaId,
        repartidorId: 'REP-02',
        version: '1.0'
      })
    }]
  });
  console.log(`[Simulador - Ausente] ${shipmentId} -> ENTREGA_FALLIDA (AUSENTE)`);
}

async function main() {
  await producer.connect();
  console.log('🚀 Simulador conectado a Kafka');

  // Para pruebas iniciales ejecuta un solo Envío Feliz y se detiene automáticamente
  console.log('Ejecutando un escenario de prueba único...');
  await runHappyPath();
  console.log('✅ Escenario finalizado.');
}

main().catch(console.error);