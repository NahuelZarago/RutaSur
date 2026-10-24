// Monitor de SLA (CU3): consumidor con estado.
//
//  · Estado interno: tabla por envío (último estado, vencimiento, alertas emitidas) construida desde
//    envios.estados y envios.promesas. Vive en memoria y se respalda en MongoDB
//    (monitor_sla_estado) antes de confirmar cada offset.
//  · Reloj: marca de agua de tiempo de evento (máximo ocurridoEn observado), persistida en
//    monitor_sla_meta. Nunca usa el reloj de la máquina.
//  · Evaluación: al recibir cada evento y, además, en una pasada periódica sobre los envíos abiertos.
//  · Tolerancia a reinicios: al arrancar recarga la tabla desde MongoDB y retoma desde su último
//    offset. Las alertas tienen eventId determinista (alerta:<envío>:<regla>): si el monitor cae
//    entre publicar y respaldar, la re-emisión se descarta aguas abajo y no se duplica.
import { ejecutarConsumidor } from '../../shared/consumidor.js';
import { GRUPOS, SUSCRIPCIONES, TOPICS } from '../../shared/eventos.js';
import { crearKafka, crearProductor } from '../../shared/kafka.js';
import { crearLogger } from '../../shared/log.js';
import { asegurarIndices, conectarMongo } from '../../shared/mongo.js';
import { iso } from '../../shared/tiempo.js';
import { evaluar, procesarEventoMonitor, requiereEvaluacion } from '../../shared/dominio/sla.js';

const SERVICIO = 'monitor-sla';
const log = crearLogger(SERVICIO);
const TICK_MS = Number(process.env.MONITOR_TICK_MS ?? 1000);

async function main() {
  const { client, db } = await conectarMongo(log);
  await asegurarIndices(db, log);
  const coleccionEstado = db.collection('monitor_sla_estado');
  const coleccionMeta = db.collection('monitor_sla_meta');
  const kafka = crearKafka(SERVICIO);
  const productor = crearProductor(kafka);
  await productor.connect();

  // ---- recuperación del estado
  const tabla = new Map();
  for await (const st of coleccionEstado.find({})) tabla.set(st._id, st);
  const meta = await coleccionMeta.findOne({ _id: 'reloj' });
  let relojMs = meta?.watermarkMs ?? 0;
  log.info('estado recuperado', { envios: tabla.size, relojSimulado: relojMs ? iso(relojMs) : null });

  // ---- exclusión mutua entre el consumo y la pasada periódica
  let cadena = Promise.resolve();
  const conCandado = (fn) => {
    const p = cadena.then(fn);
    cadena = p.catch(() => {});
    return p;
  };

  async function emitir(eventos) {
    if (!eventos.length) return;
    await productor.send({
      topic: TOPICS.ALERTAS,
      acks: -1,
      messages: eventos.map((e) => ({ key: e.shipmentId, value: JSON.stringify(e) })),
    });
    for (const e of eventos) {
      log.info(e.tipo === 'ALERTA_ABIERTA' ? 'alerta emitida' : 'alerta cerrada', {
        eventId: e.eventId,
        shipmentId: e.shipmentId,
        regla: e.regla,
        prioridad: e.prioridad ?? null,
        relojSimulado: iso(relojMs),
      });
    }
  }

  async function respaldar(st) {
    await coleccionEstado.replaceOne({ _id: st._id }, st, { upsert: true });
  }

  async function guardarReloj() {
    await coleccionMeta.updateOne({ _id: 'reloj' }, { $max: { watermarkMs: relojMs } }, { upsert: true });
  }

  await ejecutarConsumidor({
    servicio: SERVICIO,
    groupId: GRUPOS.MONITOR_SLA,
    topics: SUSCRIPCIONES[GRUPOS.MONITOR_SLA],
    kafka,
    client,
    db,
    manejar: (evento, { meta: m }) =>
      conCandado(async () => {
        relojMs = Math.max(relojMs, Date.parse(evento.ocurridoEn));
        const r = procesarEventoMonitor(tabla.get(evento.shipmentId), m.topic, evento, relojMs);
        await emitir(r.emitir); // primero publicar…
        tabla.set(r.st._id, r.st);
        await respaldar(r.st); // …después respaldar (si cae en el medio, la re-emisión es idempotente)
        await guardarReloj();
        if (!r.aplicado) return 'descartado-fuera-de-orden';
        return r.emitir.length ? `alertas:${r.emitir.map((e) => e.regla).join(',')}` : 'estado-actualizado';
      }),
  });

  // ---- pasada periódica: el paso del tiempo también dispara alertas
  setInterval(() => {
    conCandado(async () => {
      if (!relojMs) return;
      for (const st of tabla.values()) {
        if (!requiereEvaluacion(st)) continue;
        const r = evaluar(st, relojMs);
        if (!r.emitir.length) continue;
        await emitir(r.emitir);
        tabla.set(r.st._id, r.st);
        await respaldar(r.st);
      }
    }).catch((e) => log.error('error en la evaluación periódica', { error: e.message }));
  }, TICK_MS);
}

main().catch((e) => {
  log.fatal('el monitor de SLA terminó con error', { error: e.message, stack: e.stack });
  process.exit(1);
});
