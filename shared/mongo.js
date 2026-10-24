// Adaptador de MongoDB: conexión con reintentos e índices de todas las colecciones.
import { MongoClient } from 'mongodb';
import { config } from './config.js';

const espera = (ms) => new Promise((r) => setTimeout(r, ms));

export async function reintentar(fn, { intentos = 60, pausaMs = 1000, log, descripcion = 'operación' } = {}) {
  let ultimoError;
  for (let i = 1; i <= intentos; i++) {
    try {
      return await fn();
    } catch (e) {
      ultimoError = e;
      log?.warn(`${descripcion} falló, reintentando`, { intento: i, error: e.message });
      await espera(pausaMs);
    }
  }
  throw ultimoError;
}

export async function conectarMongo(log) {
  const client = new MongoClient(config.mongoUrl, { maxPoolSize: 20 });
  await reintentar(() => client.connect(), { log, descripcion: 'conexión a MongoDB' });
  const db = client.db(config.mongoDb);
  return { client, db };
}

/** Índices definidos en el modelo de datos. createIndex es idempotente: todos los servicios lo llaman. */
export const INDICES = {
  envios: [
    [{ shipmentId: 1 }, { unique: true }],
    [{ estado: 1, zonaId: 1 }],
    [{ 'promesa.venceEn': 1 }],
    [{ estadoVisible: 1 }],
    [{ 'ultimoEvento.kafkaTs': -1 }],
    [{ 'ultimoEvento.ocurridoEn': -1 }],
  ],
  alertas: [[{ estado: 1, prioridadOrden: -1, abiertaEn: 1 }], [{ shipmentId: 1 }]],
  notificaciones: [[{ shipmentId: 1 }]],
  metricas_diarias: [[{ fecha: 1, zonaId: 1, franja: 1 }, { unique: true }], [{ hubId: 1, fecha: 1 }]],
  eventos_procesados: [
    [{ consumidor: 1, eventId: 1 }, { unique: true }],
    [{ procesadoEn: 1 }, { expireAfterSeconds: 7 * 24 * 3600 }],
  ],
  monitor_sla_estado: [[{ entregado: 1 }]],
  metricas_tiempo_hub: [[{ fecha: 1, hubId: 1 }, { unique: true }]],
};

export async function asegurarIndices(db, log) {
  await reintentar(
    async () => {
      for (const [coleccion, indices] of Object.entries(INDICES)) {
        for (const [clave, opciones] of indices) {
          await db.collection(coleccion).createIndex(clave, opciones ?? {});
        }
      }
    },
    { log, descripcion: 'creación de índices' }
  );
}
