// CU3 — Riesgo de SLA. Procesamiento con estado: el monitor cruza dos streams (estados y promesas)
// en una tabla por envío y evalúa reglas que dependen del paso del tiempo.
//
// El "ahora" es siempre el reloj simulado: la marca de agua de tiempo de evento (máximo ocurridoEn
// observado). Nunca se usa el reloj de la máquina, así el factor de aceleración del simulador
// afecta a todo el sistema por igual.
import { REGLAS_SLA, VERSION_ESQUEMA, TOPICS } from '../eventos.js';
import { HORA, MIN, iso, mismoDiaLocal } from '../tiempo.js';
import { saleDelCircuito } from './reprogramacion.js';

export const UMBRAL_SIN_INGRESO_MS = 10 * HORA;
export const UMBRAL_SIN_SALIDA_MS = 4 * HORA;

const ANTES_DEL_HUB = new Set([null, 'CREADO']);
const ANTES_DEL_REPARTO = new Set([null, 'CREADO', 'INGRESADO_HUB', 'CLASIFICADO']);

export function estadoMonitorVacio(shipmentId) {
  return {
    _id: shipmentId,
    shipmentId,
    estado: null,
    secuencia: 0,
    venceEn: null,
    promesaSecuencia: 0,
    zonaId: null,
    hubId: null,
    ultimoFallo: null,
    entregado: false,
    entregadoEn: null,
    fueraDeCircuito: false,
    alertas: {},
  };
}

export function aplicarEstadoMonitor(st, ev) {
  if (ev.secuencia <= st.secuencia) return { st, aplicado: false };
  const n = { ...st, estado: ev.tipo, secuencia: ev.secuencia, zonaId: ev.zonaId, hubId: ev.hubId };
  if (ev.tipo === 'ENTREGADO') {
    n.entregado = true;
    n.entregadoEn = ev.ocurridoEn;
  }
  if (ev.tipo === 'ENTREGA_FALLIDA') {
    n.ultimoFallo = { ocurridoEn: ev.ocurridoEn, motivo: ev.motivo, intento: ev.intento };
    n.fueraDeCircuito = saleDelCircuito(ev.motivo, ev.intento);
  }
  if (ev.tipo === 'EN_REPARTO') n.fueraDeCircuito = false;
  return { st: n, aplicado: true };
}

export function aplicarPromesaMonitor(st, ev) {
  if (ev.secuencia <= st.promesaSecuencia) return { st, aplicado: false };
  return {
    st: { ...st, venceEn: ev.venceEn, promesaSecuencia: ev.secuencia, zonaId: st.zonaId ?? ev.zonaId ?? null, hubId: st.hubId ?? ev.hubId ?? null },
    aplicado: true,
  };
}

function textoRestante(ms) {
  const total = Math.max(0, Math.round(ms / MIN));
  const h = Math.floor(total / 60);
  const m = total % 60;
  return h ? `${h} h ${m} min` : `${m} min`;
}

/** Reglas que se cumplen ahora y todavía no alertaron sobre este envío. */
export function evaluarReglas(st, ahoraMs) {
  if (!st.venceEn || st.entregado) return [];
  const vence = Date.parse(st.venceEn);
  const restante = vence - ahoraMs;
  const yaAlerto = (regla) => st.alertas[regla] !== undefined;
  const nuevas = [];
  const agregar = (regla, causa) => nuevas.push({ regla, prioridad: REGLAS_SLA[regla].prioridad, causa });

  if (!st.fueraDeCircuito) {
    if (!yaAlerto('SIN_INGRESO') && ANTES_DEL_HUB.has(st.estado) && restante > 0 && restante < UMBRAL_SIN_INGRESO_MS) {
      agregar('SIN_INGRESO', `Faltan ${textoRestante(restante)} para el vencimiento y el paquete no ingresó al hub.`);
    }
    if (!yaAlerto('SIN_SALIDA') && ANTES_DEL_REPARTO.has(st.estado) && restante > 0 && restante < UMBRAL_SIN_SALIDA_MS) {
      agregar('SIN_SALIDA', `Faltan ${textoRestante(restante)} y el envío no salió a reparto (estado ${st.estado ?? 'desconocido'}).`);
    }
    if (!yaAlerto('VENCIDO') && restante <= 0) {
      agregar('VENCIDO', `Pasó el vencimiento de la promesa sin ENTREGADO (último estado: ${st.estado ?? 'desconocido'}).`);
    }
  }
  if (
    !yaAlerto('FALLO_DIA_LIMITE') &&
    st.estado === 'ENTREGA_FALLIDA' &&
    st.ultimoFallo &&
    mismoDiaLocal(st.ultimoFallo.ocurridoEn, vence)
  ) {
    agregar('FALLO_DIA_LIMITE', `Entrega fallida (${st.ultimoFallo.motivo}) el mismo día en que vence la promesa.`);
  }
  return nuevas;
}

export function eventoApertura(st, alerta, ahoraMs) {
  return {
    eventId: `alerta:${st.shipmentId}:${alerta.regla}`,
    shipmentId: st.shipmentId,
    tipo: 'ALERTA_ABIERTA',
    regla: alerta.regla,
    prioridad: alerta.prioridad,
    causa: alerta.causa,
    venceEn: st.venceEn,
    estadoEnvio: st.estado,
    zonaId: st.zonaId,
    hubId: st.hubId,
    ocurridoEn: iso(ahoraMs),
    version: VERSION_ESQUEMA,
  };
}

export function eventoCierre(st, regla) {
  return {
    eventId: `cierre:${st.shipmentId}:${regla}`,
    shipmentId: st.shipmentId,
    tipo: 'ALERTA_CERRADA',
    regla,
    motivoCierre: 'ENTREGADO',
    zonaId: st.zonaId,
    hubId: st.hubId,
    ocurridoEn: st.entregadoEn,
    version: VERSION_ESQUEMA,
  };
}

/** Abre alertas nuevas o cierra las abiertas si el envío se entregó. Devuelve los eventos a emitir. */
export function evaluar(st, ahoraMs) {
  const emitir = [];
  let n = st;
  if (st.entregado) {
    for (const [regla, a] of Object.entries(st.alertas)) {
      if (a.estado !== 'ABIERTA') continue;
      emitir.push(eventoCierre(n, regla));
      n = { ...n, alertas: { ...n.alertas, [regla]: { ...a, estado: 'CERRADA', cerradaEn: st.entregadoEn } } };
    }
    return { st: n, emitir };
  }
  for (const alerta of evaluarReglas(st, ahoraMs)) {
    emitir.push(eventoApertura(n, alerta, ahoraMs));
    n = { ...n, alertas: { ...n.alertas, [alerta.regla]: { estado: 'ABIERTA', prioridad: alerta.prioridad, abiertaEn: iso(ahoraMs) } } };
  }
  return { st: n, emitir };
}

/** Procesa un evento de estados o promesas y evalúa las reglas para ese envío. */
export function procesarEventoMonitor(actual, topic, ev, ahoraMs) {
  const st = actual ?? estadoMonitorVacio(ev.shipmentId);
  const r = topic === TOPICS.PROMESAS ? aplicarPromesaMonitor(st, ev) : aplicarEstadoMonitor(st, ev);
  const e = evaluar(r.st, ahoraMs);
  return { st: e.st, emitir: e.emitir, aplicado: r.aplicado };
}

/** ¿Hace falta seguir evaluando este envío en la pasada periódica? */
export function requiereEvaluacion(st) {
  if (st.entregado || !st.venceEn) return false;
  if (!st.fueraDeCircuito) return !st.alertas.VENCIDO;
  return st.estado === 'ENTREGA_FALLIDA' && !st.alertas.FALLO_DIA_LIMITE;
}
