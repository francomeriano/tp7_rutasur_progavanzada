# 🚚 RutaSur - Torre de Control (Event-Driven Architecture)

Sistema de monitoreo logístico en tiempo real basado en una Arquitectura Orientada a Eventos (EDA). El proyecto simula el ciclo de vida de paquetes mediante eventos en Kafka, los proyecta en MongoDB mediante un microservicio proyector, y los visualiza dinámicamente en una interfaz web con Server-Sent Events (SSE).

---

## 🛠️ Arquitectura y Tecnologías

```text
[ Simulador ] ──(Kafka Topic: envios.estados)──> [ Proyector ]
                                                      │
                                                      ▼
                                           [ MongoDB Replica Set ]
                                                      │
                                            (Change Streams)
                                                      ▼
                                              [ Next.js SSE ]
                                                      │
                                                      ▼
                                           [ Torre de Control UI ]
```

* **Event Broker:** Apache Kafka + Zookeeper (Docker)
* **Base de Datos (Read Model):** MongoDB v4.4.30 (Replica Set `rs0` habilitado para Change Streams)
* **Microservicios (Node.js):**
  * `simulator`: Genera eventos de ciclo de vida (Happy path, fallos, SLA, etc.).
  * `projector`: Consumidor Kafka que consolida el estado final en MongoDB.
* **Frontend:** Next.js 14 + Server-Sent Events (SSE) para actualizaciones sin recarga.

---

## 🚀 Requisitos Previos

* [Docker Desktop](https://www.docker.com/products/docker-desktop/) en ejecución.
* [Node.js](https://nodejs.org/) (v18 o superior) y `npm`.

---

## 📦 Puesta en Marcha

### 1. Iniciar la Infraestructura en Docker

Desde la raíz del proyecto, ejecutá:

```bash
docker-compose up -d
```

### 2. Iniciar la Aplicación

Ejecutá el siguiente comando para levantar todos los servicios y la interfaz web en paralelo:

```bash
npm run dev:all
```

---

## 💻 Visualización y Verificación

1. Abrí tu navegador en **`http://localhost:3000`**.
2. Verificá que el indicador de estado muestre **`🟢 Conectado`**.
3. Observá cómo los paquetes se actualizan dinámicamente en la tabla en tiempo real.

---

## 🔧 Solución de Problemas Frecuentes

* **Error de conexión a MongoDB (`getaddrinfo ENOTFOUND`):** Asegurate de incluir `directConnection=true` en el `MONGO_URI` cuando te conectes fuera de la red interna de Docker.
* **Error en Change Stream:** Para MongoDB 4.4, se utiliza `{ fullDocument: 'updateLookup' }` al suscribirse a la colección.