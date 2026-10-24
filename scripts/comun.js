// Utilidades compartidas por los scripts operativos.
import { SUSCRIPCIONES } from '../shared/eventos.js';
import { calcularLag } from '../shared/kafka.js';

export const espera = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Lee un topic completo, desde el inicio hasta el último offset existente al momento de la llamada,
 * con un grupo efímero (no altera los offsets de ningún servicio).
 * @returns {Promise<Array<{topic, partition, offset, key, value, timestamp}>>}
 */
export async function leerTopicCompleto(kafka, topics) {
  const admin = kafka.admin();
  await admin.connect();
  const objetivo = new Map();
  for (const topic of topics) {
    for (const p of await admin.fetchTopicOffsets(topic)) {
      if (Number(p.high) > Number(p.low)) objetivo.set(`${topic}:${p.partition}`, Number(p.high));
    }
  }
  const groupId = `herramientas-${Date.now()}`;
  const mensajes = [];
  if (objetivo.size) {
    const consumer = kafka.consumer({ groupId });
    await consumer.connect();
    await consumer.subscribe({ topics, fromBeginning: true });
    await new Promise((resolve, reject) => {
      const pendientes = new Set(objetivo.keys());
      consumer
        .run({
          autoCommit: false,
          eachMessage: async ({ topic, partition, message }) => {
            const clave = `${topic}:${partition}`;
            if (!objetivo.has(clave) || Number(message.offset) >= objetivo.get(clave)) return;
            mensajes.push({
              topic,
              partition,
              offset: message.offset,
              key: message.key?.toString() ?? null,
              value: message.value?.toString('utf8') ?? '',
              timestamp: Number(message.timestamp),
            });
            if (Number(message.offset) >= objetivo.get(clave) - 1) {
              pendientes.delete(clave);
              if (pendientes.size === 0) resolve();
            }
          },
        })
        .catch(reject);
    });
    await consumer.disconnect();
    await admin.deleteGroups([groupId]).catch(() => {});
  }
  await admin.disconnect();
  return mensajes;
}

/** Espera a que un grupo no tenga lag (o hasta agotar el tiempo). */
export async function esperarLagCero(kafka, groupId, { maxMs = 120_000 } = {}) {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const inicio = Date.now();
    for (;;) {
      const lag = await calcularLag(admin, groupId, SUSCRIPCIONES[groupId]);
      if (lag.lagTotal === 0) return lag;
      if (Date.now() - inicio > maxMs) throw new Error(`El grupo ${groupId} sigue con lag ${lag.lagTotal}`);
      process.stdout.write(`  esperando a ${groupId}: lag ${lag.lagTotal}\r`);
      await espera(1000);
    }
  } finally {
    await admin.disconnect();
  }
}

export async function llamarSimulador(base, ruta) {
  try {
    const r = await fetch(`${base}/${ruta}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' });
    return r.ok;
  } catch {
    return false;
  }
}
