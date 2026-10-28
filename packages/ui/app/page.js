'use client';

import { useEffect, useState } from 'react';

export default function TorreDeControl() {
  const [envios, setEnvios] = useState([]);
  const [conectado, setConectado] = useState(false);

  useEffect(() => {
    const eventSource = new EventSource('/api/sse');

    eventSource.onopen = () => {
      setConectado(true);
    };

    eventSource.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);

        if (payload.type === 'INIT') {
          setEnvios(payload.data || []);
        } else if (payload.type === 'UPDATE') {
          const itemActualizado = payload.data;
          setEnvios((prev) => {
            const index = prev.findIndex((e) => e.shipmentId === itemActualizado.shipmentId);
            if (index !== -1) {
              const copia = [...prev];
              copia[index] = itemActualizado;
              return copia;
            }
            return [itemActualizado, ...prev];
          });
        }
      } catch (err) {
        console.error('Error parseando evento SSE:', err);
      }
    };

    eventSource.onerror = () => {
      setConectado(false);
    };

    return () => {
      eventSource.close();
    };
  }, []);

  return (
    <div style={{ padding: '2rem', fontFamily: 'sans-serif' }}>
      <h1>🚚 RutaSur - Torre de Control</h1>
      <div style={{ marginBottom: '1rem' }}>
        Estado SSE: {conectado ? <span style={{ color: 'green', fontWeight: 'bold' }}>🟢 Conectado</span> : <span style={{ color: 'red', fontWeight: 'bold' }}>🔴 Desconectado</span>}
      </div>

      <table border="1" cellPadding="8" cellSpacing="0" style={{ width: '100%', textAlign: 'left', borderCollapse: 'collapse' }}>
        <thead>
          <tr style={{ backgroundColor: '#f2f2f2' }}>
            <th>Shipment ID</th>
            <th>Estado Actual</th>
            <th>Hub</th>
            <th>Zona</th>
            <th>Última Secuencia</th>
          </tr>
        </thead>
        <tbody>
          {envios.length === 0 ? (
            <tr>
              <td colSpan="5" style={{ textAlign: 'center' }}>Esperando eventos del simulador...</td>
            </tr>
          ) : (
            envios.map((envio) => (
              <tr key={envio.shipmentId}>
                <td><strong>{envio.shipmentId}</strong></td>
                <td>{envio.estadoActual}</td>
                <td>{envio.hubId}</td>
                <td>{envio.zonaId}</td>
                <td>{envio.ultimaSecuencia}</td>
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}
