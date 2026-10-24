// Validación de esquema de cada evento, sin dependencias externas.
// Cada consumidor valida lo que recibe; si un mensaje no cumple, va a la DLQ con el motivo.
import {
  MOTIVOS_FALLO,
  REGLAS_SLA,
  TIPOS_CON_INTENTO,
  TIPOS_ESTADO,
  TIPOS_REPROGRAMACION,
  TOPICS,
  VERSION_ESQUEMA,
} from './eventos.js';
import { FRANJAS } from './tiempo.js';

const esTexto = (v) => typeof v === 'string' && v.trim().length > 0;
const esEntero = (v) => Number.isInteger(v);
const esFecha = (v) => typeof v === 'string' && v.length >= 10 && !Number.isNaN(Date.parse(v));

function validarBase(e, errores) {
  if (e === null || typeof e !== 'object' || Array.isArray(e)) {
    errores.push('el mensaje no es un objeto JSON');
    return false;
  }
  if (!esTexto(e.eventId)) errores.push('eventId ausente o vacío');
  if (!esTexto(e.shipmentId)) errores.push('shipmentId ausente o vacío');
  if (!esTexto(e.tipo)) errores.push('tipo ausente');
  if (!esFecha(e.ocurridoEn)) errores.push('ocurridoEn no es una fecha ISO válida');
  if (e.version !== VERSION_ESQUEMA) errores.push(`version de esquema no soportada (${e.version})`);
  return true;
}

function validarEstado(e, errores) {
  if (!validarBase(e, errores)) return;
  if (!TIPOS_ESTADO.includes(e.tipo)) errores.push(`tipo de estado desconocido: ${e.tipo}`);
  if (!esEntero(e.secuencia) || e.secuencia < 1) errores.push('secuencia debe ser un entero >= 1');
  if (!esTexto(e.hubId)) errores.push('hubId ausente');
  if (!esTexto(e.zonaId)) errores.push('zonaId ausente');
  if (e.repartidorId !== undefined && e.repartidorId !== null && !esTexto(e.repartidorId)) {
    errores.push('repartidorId debe ser texto o null');
  }
  if (TIPOS_CON_INTENTO.includes(e.tipo) && (!esEntero(e.intento) || e.intento < 1)) {
    errores.push(`${e.tipo} requiere intento (entero >= 1)`);
  }
  if (e.tipo === 'ENTREGA_FALLIDA' && !MOTIVOS_FALLO.includes(e.motivo)) {
    errores.push(`ENTREGA_FALLIDA requiere motivo válido (${MOTIVOS_FALLO.join(', ')})`);
  }
  if (e.franja !== undefined && !FRANJAS[e.franja]) errores.push(`franja desconocida: ${e.franja}`);
}

function validarPromesa(e, errores) {
  if (!validarBase(e, errores)) return;
  if (e.tipo !== 'PROMESA') errores.push('tipo debe ser PROMESA');
  if (!esFecha(e.venceEn)) errores.push('venceEn no es una fecha ISO válida');
  if (!esEntero(e.secuencia) || e.secuencia < 1) errores.push('secuencia debe ser un entero >= 1');
}

function validarReprogramacion(e, errores) {
  if (!validarBase(e, errores)) return;
  if (!TIPOS_REPROGRAMACION.includes(e.tipo)) errores.push(`tipo de reprogramación desconocido: ${e.tipo}`);
  if (!esTexto(e.causaEventId)) errores.push('causaEventId ausente');
  if (!esEntero(e.intento)) errores.push('intento ausente');
  if (e.tipo === 'REPROGRAMADO') {
    const p = e.proximoIntento;
    if (!p || !esTexto(p.fecha) || !FRANJAS[p.franja] || !esFecha(p.desde)) {
      errores.push('REPROGRAMADO requiere proximoIntento { fecha, franja, desde }');
    }
  }
}

function validarAlerta(e, errores) {
  if (!validarBase(e, errores)) return;
  if (!['ALERTA_ABIERTA', 'ALERTA_CERRADA'].includes(e.tipo)) errores.push(`tipo de alerta desconocido: ${e.tipo}`);
  if (!REGLAS_SLA[e.regla]) errores.push(`regla desconocida: ${e.regla}`);
  if (e.tipo === 'ALERTA_ABIERTA' && REGLAS_SLA[e.regla] && e.prioridad !== REGLAS_SLA[e.regla].prioridad) {
    errores.push('prioridad no coincide con la regla');
  }
}

function validarNotificacion(e, errores) {
  if (!validarBase(e, errores)) return;
  if (e.tipo !== 'NOTIFICACION') errores.push('tipo debe ser NOTIFICACION');
  if (!esTexto(e.causaEventId)) errores.push('causaEventId ausente');
  if (!esTexto(e.plantilla)) errores.push('plantilla ausente');
  if (!['SMS', 'EMAIL'].includes(e.canal)) errores.push('canal inválido');
  if (!esTexto(e.mensaje)) errores.push('mensaje ausente');
}

const VALIDADORES = {
  [TOPICS.ESTADOS]: validarEstado,
  [TOPICS.PROMESAS]: validarPromesa,
  [TOPICS.REPROGRAMACIONES]: validarReprogramacion,
  [TOPICS.ALERTAS]: validarAlerta,
  [TOPICS.NOTIFICACIONES]: validarNotificacion,
};

/** Devuelve la lista de errores (vacía si el evento es válido). */
export function validarEvento(topic, evento) {
  const validador = VALIDADORES[topic];
  if (!validador) return [`no hay esquema para el topic ${topic}`];
  const errores = [];
  validador(evento, errores);
  return errores;
}

/**
 * Decodifica un mensaje de Kafka (clave y valor como Buffer o string) y lo valida.
 * @returns {{ok: true, evento: object, texto: string} | {ok: false, errores: string[], texto: string}}
 */
export function decodificarMensaje(topic, clave, valor) {
  const texto = valor == null ? '' : valor.toString('utf8');
  let evento;
  try {
    evento = JSON.parse(texto);
  } catch (e) {
    return { ok: false, errores: [`JSON inválido: ${e.message}`], texto };
  }
  const errores = validarEvento(topic, evento);
  const claveTexto = clave == null ? null : clave.toString();
  if (errores.length === 0 && claveTexto !== null && claveTexto !== evento.shipmentId) {
    errores.push(`la clave del mensaje (${claveTexto}) no coincide con shipmentId (${evento.shipmentId})`);
  }
  return errores.length ? { ok: false, errores, texto } : { ok: true, evento, texto };
}
