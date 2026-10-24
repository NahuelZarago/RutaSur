// Consultas de lectura sobre las vistas que mantienen el proyector y la analítica.
import { obtenerDb } from './mongo';

export function resumenEnvio(doc) {
  return {
    shipmentId: doc.shipmentId ?? doc._id,
    estadoVisible: doc.estadoVisible ?? 'SIN_ESTADO',
    zonaId: doc.zonaId ?? null,
    eventId: doc.ultimoEvento?.eventId ?? null,
    tipo: doc.ultimoEvento?.tipo ?? null,
    motivo: doc.ultimoEvento?.motivo ?? null,
    ocurridoEn: doc.ultimoEvento?.ocurridoEn ?? null,
    kafkaTs: doc.ultimoEvento?.kafkaTs ?? null,
  };
}

export function resumenAlerta(doc) {
  return {
    _id: doc._id,
    shipmentId: doc.shipmentId,
    regla: doc.regla,
    prioridad: doc.prioridad,
    prioridadOrden: doc.prioridadOrden,
    causa: doc.causa,
    estado: doc.estado,
    abiertaEn: doc.abiertaEn,
    venceEn: doc.venceEn,
    zonaId: doc.zonaId,
  };
}

export async function contadores() {
  const db = await obtenerDb();
  const filas = await db
    .collection('envios')
    .aggregate([{ $group: { _id: '$estadoVisible', n: { $sum: 1 } } }])
    .toArray();
  return Object.fromEntries(filas.map((f) => [f._id ?? 'SIN_ESTADO', f.n]));
}

export async function alertasAbiertas(limite = 60) {
  const db = await obtenerDb();
  const docs = await db
    .collection('alertas')
    .find({ estado: 'ABIERTA' })
    .sort({ prioridadOrden: -1, abiertaEn: 1 })
    .limit(limite)
    .toArray();
  return docs.map(resumenAlerta);
}

export async function ultimosEventos(limite = 15) {
  const db = await obtenerDb();
  const docs = await db
    .collection('envios')
    .find({ ultimoEvento: { $ne: null } }, { projection: { shipmentId: 1, estadoVisible: 1, zonaId: 1, ultimoEvento: 1 } })
    .sort({ 'ultimoEvento.kafkaTs': -1 })
    .limit(limite)
    .toArray();
  return docs.map(resumenEnvio);
}

export async function relojSimulado() {
  const db = await obtenerDb();
  const doc = await db
    .collection('envios')
    .find({ ultimoEvento: { $ne: null } }, { projection: { 'ultimoEvento.ocurridoEn': 1 } })
    .sort({ 'ultimoEvento.ocurridoEn': -1 })
    .limit(1)
    .next();
  return doc?.ultimoEvento?.ocurridoEn ?? null;
}

export async function datosTorre() {
  const [c, a, e, r] = await Promise.all([contadores(), alertasAbiertas(), ultimosEventos(), relojSimulado()]);
  return { contadores: c, alertas: a, eventos: e, reloj: r };
}

export async function detalleEnvio(id) {
  const db = await obtenerDb();
  const [envio, alertas, notificaciones] = await Promise.all([
    db.collection('envios').findOne({ _id: id }),
    db.collection('alertas').find({ shipmentId: id }).sort({ abiertaEn: 1 }).toArray(),
    db.collection('notificaciones').find({ shipmentId: id }).sort({ ocurridoEn: 1 }).toArray(),
  ]);
  if (!envio) return null;
  return { envio, alertas, notificaciones };
}

/** Versión pública: sin hub, zona interna, repartidor ni alertas. */
export async function trackingPublico(id) {
  const db = await obtenerDb();
  const e = await db.collection('envios').findOne({ _id: id });
  if (!e || !e.estado) return null;
  return {
    shipmentId: e.shipmentId,
    estadoVisible: e.estadoVisible,
    comercio: e.comercio,
    localidad: e.destino?.localidad ?? null,
    entregadoEn: e.entregadoEn,
    promesa: e.promesa ? { venceEn: e.promesa.venceEn } : null,
    resolucion: e.resolucion ? { tipo: e.resolucion.tipo, proximoIntento: e.resolucion.proximoIntento } : null,
    ultimoFallo: e.ultimoFallo ? { motivo: e.ultimoFallo.motivo } : null,
    pasos: e.historial.map((h) => ({ tipo: h.tipo, ocurridoEn: h.ocurridoEn, eventId: h.eventId })),
  };
}

export async function listarEnvios({ estado, zona, q, limite = 100 }) {
  const db = await obtenerDb();
  const filtro = {};
  if (estado) filtro.estadoVisible = estado;
  if (zona) filtro.zonaId = zona;
  if (q) filtro._id = { $regex: q.replace(/[^\w-]/g, ''), $options: 'i' };
  return db
    .collection('envios')
    .find(filtro, { projection: { historial: 0, reprogramaciones: 0 } })
    .sort({ 'ultimoEvento.kafkaTs': -1 })
    .limit(limite)
    .toArray();
}

/** Indicadores de CU4: agregaciones sobre metricas_diarias (ya precalculadas). */
export async function datosAnalitica({ hub, zona, desde, hasta }) {
  const db = await obtenerDb();
  const match = {};
  if (hub) match.hubId = hub;
  if (zona) match.zonaId = zona;
  if (desde || hasta) match.fecha = { ...(desde && { $gte: desde }), ...(hasta && { $lte: hasta }) };
  const metricas = db.collection('metricas_diarias');
  const matchTiempo = { ...(hub && { hubId: hub }), ...(match.fecha && { fecha: match.fecha }) };

  const [zonaFranja, porDia, motivos, sla, tiempos, fechas] = await Promise.all([
    metricas
      .aggregate([
        { $match: match },
        {
          $group: {
            _id: { zonaId: '$zonaId', franja: '$franja' },
            total: { $sum: '$primerIntento.total' },
            exitosos: { $sum: '$primerIntento.exitosos' },
          },
        },
      ])
      .toArray(),
    metricas
      .aggregate([
        { $match: match },
        { $group: { _id: '$fecha', total: { $sum: '$primerIntento.total' }, exitosos: { $sum: '$primerIntento.exitosos' } } },
        { $sort: { _id: 1 } },
      ])
      .toArray(),
    metricas
      .aggregate([
        { $match: match },
        {
          $group: {
            _id: '$zonaId',
            AUSENTE: { $sum: '$fallos.AUSENTE' },
            DIRECCION_INCORRECTA: { $sum: '$fallos.DIRECCION_INCORRECTA' },
            RECHAZADO: { $sum: '$fallos.RECHAZADO' },
            ZONA_INACCESIBLE: { $sum: '$fallos.ZONA_INACCESIBLE' },
          },
        },
      ])
      .toArray(),
    metricas
      .aggregate([
        { $match: match },
        {
          $group: {
            _id: { hubId: '$hubId', fecha: '$fecha' },
            conPromesa: { $sum: '$entregas.conPromesa' },
            enPlazo: { $sum: '$entregas.enPlazo' },
          },
        },
        { $sort: { '_id.fecha': 1 } },
      ])
      .toArray(),
    db
      .collection('metricas_tiempo_hub')
      .aggregate([
        { $match: matchTiempo },
        { $group: { _id: { hubId: '$hubId', fecha: '$fecha' }, sumaMinutos: { $sum: '$sumaMinutos' }, cantidad: { $sum: '$cantidad' } } },
        { $sort: { '_id.fecha': 1 } },
      ])
      .toArray(),
    metricas.distinct('fecha'),
  ]);
  return { zonaFranja, porDia, motivos, sla, tiempos, fechas: fechas.sort() };
}
