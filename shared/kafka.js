// Adaptador de Kafka (kafkajs): creación del cliente, topics y cálculo de lag por grupo.
import { Kafka, logLevel, Partitioners } from 'kafkajs';
import { config } from './config.js';
import { DEFINICION_TOPICS, SUSCRIPCIONES, TOPICS } from './eventos.js';

export function crearKafka(clientId) {
  return new Kafka({
    clientId,
    brokers: config.kafkaBrokers,
    logLevel: logLevel.WARN,
    retry: { initialRetryTime: 300, retries: 12 },
  });
}

/**
 * Productor con orden estricto por partición: un solo request en vuelo evita que un reintento
 * reordene mensajes del mismo envío. El particionador por defecto (murmur2) es compatible con Java.
 */
export function crearProductor(kafka) {
  return kafka.producer({
    allowAutoTopicCreation: false,
    maxInFlightRequests: 1,
    createPartitioner: Partitioners.DefaultPartitioner,
  });
}

export async function asegurarTopics(kafka) {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const existentes = new Set(await admin.listTopics());
    const faltantes = DEFINICION_TOPICS.filter((t) => !existentes.has(t.topic));
    if (faltantes.length) await admin.createTopics({ topics: faltantes, waitForLeaders: true });
    return { creados: faltantes.map((t) => t.topic), existentes: [...existentes] };
  } finally {
    await admin.disconnect();
  }
}

/** Lag de un grupo: mensajes en el log que el grupo todavía no confirmó. */
export async function calcularLag(admin, groupId, topics = SUSCRIPCIONES[groupId]) {
  const [descripcion] = (await admin.describeGroups([groupId])).groups;
  const comprometidos = await admin.fetchOffsets({ groupId, topics });
  const resultado = {
    groupId,
    estado: descripcion?.state ?? 'Desconocido',
    miembros: descripcion?.members?.length ?? 0,
    lagTotal: 0,
    iniciado: false,
    topics: [],
  };
  for (const { topic, partitions } of comprometidos) {
    const fin = await admin.fetchTopicOffsets(topic);
    const detalle = { topic, lag: 0, particiones: [] };
    for (const p of fin) {
      const comprometido = partitions.find((x) => x.partition === p.partition);
      const offset = comprometido ? Number(comprometido.offset) : -1;
      const high = Number(p.high);
      const low = Number(p.low);
      if (offset >= 0) resultado.iniciado = true;
      const lag = offset < 0 ? high - low : Math.max(0, high - offset);
      detalle.lag += lag;
      detalle.particiones.push({ particion: p.partition, offset, high, lag });
    }
    resultado.lagTotal += detalle.lag;
    resultado.topics.push(detalle);
  }
  return resultado;
}

/** Cantidad de mensajes presentes en la DLQ. */
export async function contarDlq(admin) {
  const offsets = await admin.fetchTopicOffsets(TOPICS.DLQ);
  return offsets.reduce((s, p) => s + (Number(p.high) - Number(p.low)), 0);
}

export function headersATexto(headers = {}) {
  const salida = {};
  for (const [k, v] of Object.entries(headers)) salida[k] = v == null ? null : v.toString();
  return salida;
}
