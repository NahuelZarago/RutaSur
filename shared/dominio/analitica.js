// CU4 — Analítica incremental. Cada evento se traduce en incrementos atómicos ($inc) sobre
// metricas_diarias, una fila por (fecha, zona, franja). Así el tablero nunca recalcula sobre
// millones de documentos: agrega filas ya precalculadas.
//
// Indicadores:
//  1. Tasa de entrega en primer intento  → primerIntento.{total, exitosos}   (zona, franja, día)
//  2. Distribución de motivos de fallo   → fallos.{total, AUSENTE, ...}      (zona, motivo)
//  3. Cumplimiento de SLA                → entregas.{total, conPromesa, enPlazo} (hub, día)
//
// El cumplimiento de SLA necesita la promesa: el servicio cruza envios.estados con
// envios.promesas mediante un "join" simétrico persistido (analitica_promesas). Cuenta la entrega
// cuando conoce ambos lados, llegue primero el que llegue.
import { TOPICS } from '../eventos.js';
import { fechaLocal, franjaDe } from '../tiempo.js';

export function claveMetrica(ev) {
  return {
    fecha: fechaLocal(ev.ocurridoEn),
    zonaId: ev.zonaId,
    hubId: ev.hubId,
    franja: ev.franja ?? franjaDe(ev.ocurridoEn),
  };
}

export const idMetrica = (c) => `${c.fecha}|${c.zonaId}|${c.franja}`;

export function incrementosPorEstado(ev) {
  if (ev.tipo === 'ENTREGA_FALLIDA') {
    const inc = { 'fallos.total': 1, [`fallos.${ev.motivo}`]: 1 };
    if (ev.intento === 1) inc['primerIntento.total'] = 1;
    return [{ clave: claveMetrica(ev), inc }];
  }
  if (ev.tipo === 'ENTREGADO') {
    const inc = { 'entregas.total': 1 };
    if (ev.intento === 1) {
      inc['primerIntento.total'] = 1;
      inc['primerIntento.exitosos'] = 1;
    }
    return [{ clave: claveMetrica(ev), inc }];
  }
  return [];
}

export function joinVacio(shipmentId) {
  return { _id: shipmentId, venceEn: null, promesaSecuencia: 0, entrega: null, contado: false };
}

export function aplicarJoinSla(actual, topic, ev) {
  const join = { ...(actual ?? joinVacio(ev.shipmentId)) };
  if (topic === TOPICS.PROMESAS && ev.secuencia > join.promesaSecuencia) {
    join.venceEn = ev.venceEn;
    join.promesaSecuencia = ev.secuencia;
  }
  if (topic === TOPICS.ESTADOS && ev.tipo === 'ENTREGADO' && !join.entrega) {
    join.entrega = { ocurridoEn: ev.ocurridoEn, ...claveMetrica(ev) };
  }
  const incrementos = [];
  if (!join.contado && join.venceEn && join.entrega) {
    join.contado = true;
    const enPlazo = Date.parse(join.entrega.ocurridoEn) <= Date.parse(join.venceEn);
    const { ocurridoEn, ...clave } = join.entrega;
    incrementos.push({ clave, inc: { 'entregas.conPromesa': 1, 'entregas.enPlazo': enPlazo ? 1 : 0 } });
  }
  return { join, incrementos };
}

/** ¿Este evento necesita leer/escribir el join de SLA? */
export const participaEnJoinSla = (topic, ev) =>
  topic === TOPICS.PROMESAS || (topic === TOPICS.ESTADOS && ev.tipo === 'ENTREGADO');

// ---------------------------------------------------------------- 4.º indicador (reprocesamiento)
// Tiempo medio entre INGRESADO_HUB y EN_REPARTO (primer intento) por hub. Lo calcula un grupo de
// consumidores nuevo que lee envios.estados desde el inicio.

export function pendienteTiemposVacio(shipmentId) {
  return { _id: shipmentId, ingreso: null, salida: null, hubId: null, contado: false };
}

export function aplicarTiemposHub(actual, ev) {
  const p = { ...(actual ?? pendienteTiemposVacio(ev.shipmentId)) };
  if (ev.tipo === 'INGRESADO_HUB' && !p.ingreso) {
    p.ingreso = ev.ocurridoEn;
    p.hubId = ev.hubId;
  }
  if (ev.tipo === 'EN_REPARTO' && ev.intento === 1 && !p.salida) {
    p.salida = ev.ocurridoEn;
    p.hubId = p.hubId ?? ev.hubId;
  }
  let incremento = null;
  if (!p.contado && p.ingreso && p.salida) {
    p.contado = true;
    const minutos = Math.max(0, Math.round((Date.parse(p.salida) - Date.parse(p.ingreso)) / 60_000));
    incremento = { clave: { fecha: fechaLocal(p.salida), hubId: p.hubId }, inc: { sumaMinutos: minutos, cantidad: 1 } };
  }
  return { pendiente: p, incremento };
}

export const idMetricaTiempo = (c) => `${c.fecha}|${c.hubId}`;
