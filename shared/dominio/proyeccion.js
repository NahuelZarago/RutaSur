// CU1 — Proyección del estado de cada envío (lado de lectura de CQRS).
//
// Funciones puras: reciben el documento actual (o null) y un evento y devuelven el documento nuevo.
// El servicio proyector las aplica dentro de una transacción (leer → reducir → reemplazar).
//
// Diseño para la reconstrucción: cada topic escribe campos disjuntos del documento y las
// combinaciones son conmutativas (máximos, conjuntos ordenados, contadores con deduplicación).
// Así, borrar las colecciones y reprocesar Kafka desde el inicio deja exactamente el mismo
// resultado, aunque los distintos topics se intercalen en otro orden.
import { HEADER_RELOJ, ORDEN_PRIORIDAD, REGLAS_SLA, TOPICS } from '../eventos.js';

export function envioVacio(shipmentId) {
  return {
    _id: shipmentId,
    shipmentId,
    estado: null,
    estadoVisible: 'SIN_ESTADO',
    ultimaSecuencia: 0,
    historial: [],
    intentos: 0,
    hubId: null,
    zonaId: null,
    repartidorId: null,
    comercio: null,
    destino: null,
    creadoEn: null,
    entregadoEn: null,
    ultimoFallo: null,
    ultimoEvento: null,
    promesa: null,
    reprogramaciones: [],
    resolucion: null,
    alertas: {},
    notificaciones: { total: 0, ultima: null },
    descartadosFueraDeOrden: 0,
    actualizadoKafkaTs: 0,
  };
}

/** Estado que ve el supervisor: un fallo ya resuelto muestra su resolución. */
export function derivarEstadoVisible(envio) {
  if (!envio.estado) return 'SIN_ESTADO';
  if (envio.estado === 'ENTREGA_FALLIDA') {
    const resolucion = envio.reprogramaciones.find((r) => r.intento === envio.intentos);
    if (resolucion) return resolucion.tipo;
  }
  return envio.estado;
}

const conTs = (envio, meta) => Math.max(envio.actualizadoKafkaTs ?? 0, meta.kafkaTs ?? 0);

/**
 * Aplica un evento de envios.estados.
 *  · Regla de orden: secuencia menor o igual a la ya aplicada → se descarta (no retrocede).
 *  · La regla de duplicados (eventId) la resuelve antes el ejecutor con eventos_procesados; un
 *    duplicado que llegara hasta acá también cae por la regla de orden.
 */
export function aplicarEstado(actual, ev, meta = {}) {
  const envio = actual ?? envioVacio(ev.shipmentId);
  if (ev.secuencia <= envio.ultimaSecuencia) {
    return {
      aplicado: false,
      envio: { ...envio, descartadosFueraDeOrden: envio.descartadosFueraDeOrden + 1, actualizadoKafkaTs: conTs(envio, meta) },
    };
  }

  const relojPublicacion = meta.headers?.[HEADER_RELOJ];
  const demoraMin = relojPublicacion ? Math.round((Date.parse(relojPublicacion) - Date.parse(ev.ocurridoEn)) / 60_000) : 0;

  const entrada = {
    eventId: ev.eventId,
    tipo: ev.tipo,
    secuencia: ev.secuencia,
    ocurridoEn: ev.ocurridoEn,
    kafkaTs: meta.kafkaTs ?? null,
    ...(ev.intento !== undefined && { intento: ev.intento }),
    ...(ev.motivo && { motivo: ev.motivo }),
    ...(ev.franja && { franja: ev.franja }),
    ...(ev.repartidorId && { repartidorId: ev.repartidorId }),
    ...(demoraMin > 0 && { demoraSincronizacionMin: demoraMin }),
  };

  const nuevo = {
    ...envio,
    estado: ev.tipo,
    ultimaSecuencia: ev.secuencia,
    historial: [...envio.historial, entrada],
    hubId: ev.hubId,
    zonaId: ev.zonaId,
    repartidorId: ev.repartidorId ?? envio.repartidorId,
    ultimoEvento: {
      eventId: ev.eventId,
      tipo: ev.tipo,
      secuencia: ev.secuencia,
      ocurridoEn: ev.ocurridoEn,
      kafkaTs: meta.kafkaTs ?? null,
      ...(ev.motivo && { motivo: ev.motivo }),
    },
    actualizadoKafkaTs: conTs(envio, meta),
  };

  switch (ev.tipo) {
    case 'CREADO':
      nuevo.creadoEn = ev.ocurridoEn;
      nuevo.comercio = ev.comercio ?? envio.comercio;
      nuevo.destino = ev.destino ?? envio.destino;
      break;
    case 'EN_REPARTO':
      nuevo.intentoEnCurso = { intento: ev.intento, franja: ev.franja ?? null, desde: ev.ocurridoEn };
      break;
    case 'ENTREGA_FALLIDA':
      nuevo.intentos = ev.intento;
      nuevo.ultimoFallo = { motivo: ev.motivo, intento: ev.intento, ocurridoEn: ev.ocurridoEn };
      nuevo.intentoEnCurso = null;
      break;
    case 'ENTREGADO':
      nuevo.intentos = ev.intento;
      nuevo.entregadoEn = ev.ocurridoEn;
      nuevo.intentoEnCurso = null;
      break;
    default:
      break;
  }
  nuevo.estadoVisible = derivarEstadoVisible(nuevo);
  return { aplicado: true, envio: nuevo };
}

/** envios.promesas: se queda con la versión de promesa más reciente (topic compactado). */
export function aplicarPromesa(actual, ev, meta = {}) {
  const envio = actual ?? envioVacio(ev.shipmentId);
  const vigente = envio.promesa;
  if (vigente && ev.secuencia <= vigente.secuencia) {
    return { aplicado: false, envio: { ...envio, actualizadoKafkaTs: conTs(envio, meta) } };
  }
  return {
    aplicado: true,
    envio: {
      ...envio,
      promesa: { venceEn: ev.venceEn, secuencia: ev.secuencia, eventId: ev.eventId },
      hubId: envio.hubId ?? ev.hubId ?? null,
      zonaId: envio.zonaId ?? ev.zonaId ?? null,
      actualizadoKafkaTs: conTs(envio, meta),
    },
  };
}

/** envios.reprogramaciones: una entrada por fallo (clave causaEventId), ordenadas por intento. */
export function aplicarReprogramacion(actual, ev, meta = {}) {
  const envio = actual ?? envioVacio(ev.shipmentId);
  const entrada = {
    eventId: ev.eventId,
    causaEventId: ev.causaEventId,
    tipo: ev.tipo,
    regla: ev.regla ?? null,
    detalle: ev.detalle ?? null,
    motivo: ev.motivo ?? null,
    intento: ev.intento,
    proximoIntento: ev.proximoIntento ?? null,
    ocurridoEn: ev.ocurridoEn,
  };
  const reprogramaciones = envio.reprogramaciones
    .filter((r) => r.causaEventId !== ev.causaEventId)
    .concat(entrada)
    .sort((a, b) => a.intento - b.intento || a.causaEventId.localeCompare(b.causaEventId));
  const nuevo = {
    ...envio,
    reprogramaciones,
    resolucion: reprogramaciones[reprogramaciones.length - 1],
    actualizadoKafkaTs: conTs(envio, meta),
  };
  nuevo.estadoVisible = derivarEstadoVisible(nuevo);
  return { aplicado: true, envio: nuevo };
}

/** alertas.sla → resumen embebido en el envío (alimenta el change stream de envios). */
export function aplicarAlertaEnEnvio(actual, ev, meta = {}) {
  const envio = actual ?? envioVacio(ev.shipmentId);
  const previa = envio.alertas[ev.regla] ?? {};
  const resumen =
    ev.tipo === 'ALERTA_ABIERTA'
      ? { prioridad: ev.prioridad, estado: previa.estado === 'CERRADA' ? 'CERRADA' : 'ABIERTA', abiertaEn: ev.ocurridoEn, ...(previa.cerradaEn && { cerradaEn: previa.cerradaEn }) }
      : { ...previa, prioridad: previa.prioridad ?? REGLAS_SLA[ev.regla].prioridad, estado: 'CERRADA', cerradaEn: ev.ocurridoEn };
  return {
    aplicado: true,
    envio: { ...envio, alertas: { ...envio.alertas, [ev.regla]: resumen }, actualizadoKafkaTs: conTs(envio, meta) },
  };
}

/** notificaciones.enviadas → contador y última notificación embebidos en el envío. */
export function aplicarNotificacionEnEnvio(actual, ev, meta = {}) {
  const envio = actual ?? envioVacio(ev.shipmentId);
  return {
    aplicado: true,
    envio: {
      ...envio,
      notificaciones: {
        total: envio.notificaciones.total + 1,
        ultima: { eventId: ev.eventId, plantilla: ev.plantilla, canal: ev.canal, ocurridoEn: ev.ocurridoEn },
      },
      actualizadoKafkaTs: conTs(envio, meta),
    },
  };
}

/** Despacha según el topic de origen. */
export function reducirEnvio(actual, topic, ev, meta = {}) {
  switch (topic) {
    case TOPICS.ESTADOS: {
      const r = aplicarEstado(actual, ev, meta);
      return { envio: r.envio, resultado: r.aplicado ? 'aplicado' : 'descartado-fuera-de-orden' };
    }
    case TOPICS.PROMESAS: {
      const r = aplicarPromesa(actual, ev, meta);
      return { envio: r.envio, resultado: r.aplicado ? 'promesa-aplicada' : 'promesa-vieja' };
    }
    case TOPICS.REPROGRAMACIONES:
      return { envio: aplicarReprogramacion(actual, ev, meta).envio, resultado: 'reprogramacion-aplicada' };
    case TOPICS.ALERTAS:
      return { envio: aplicarAlertaEnEnvio(actual, ev, meta).envio, resultado: 'alerta-aplicada' };
    case TOPICS.NOTIFICACIONES:
      return { envio: aplicarNotificacionEnEnvio(actual, ev, meta).envio, resultado: 'notificacion-aplicada' };
    default:
      throw new Error(`El proyector no maneja el topic ${topic}`);
  }
}

// ---------------------------------------------------------------- colección alertas

export const idAlerta = (ev) => `${ev.shipmentId}:${ev.regla}`;

export function reducirAlerta(actual, ev) {
  if (ev.tipo === 'ALERTA_ABIERTA') {
    return {
      _id: idAlerta(ev),
      shipmentId: ev.shipmentId,
      regla: ev.regla,
      prioridad: ev.prioridad,
      prioridadOrden: ORDEN_PRIORIDAD[ev.prioridad],
      causa: ev.causa ?? null,
      estado: actual?.estado === 'CERRADA' ? 'CERRADA' : 'ABIERTA',
      abiertaEn: ev.ocurridoEn,
      cerradaEn: actual?.cerradaEn ?? null,
      venceEn: ev.venceEn ?? null,
      estadoEnvio: ev.estadoEnvio ?? null,
      zonaId: ev.zonaId ?? null,
      hubId: ev.hubId ?? null,
      eventIdApertura: ev.eventId,
    };
  }
  const prioridad = actual?.prioridad ?? REGLAS_SLA[ev.regla].prioridad;
  return {
    _id: idAlerta(ev),
    shipmentId: ev.shipmentId,
    regla: ev.regla,
    prioridad,
    prioridadOrden: ORDEN_PRIORIDAD[prioridad],
    causa: actual?.causa ?? null,
    abiertaEn: actual?.abiertaEn ?? null,
    venceEn: actual?.venceEn ?? null,
    estadoEnvio: actual?.estadoEnvio ?? null,
    zonaId: actual?.zonaId ?? ev.zonaId ?? null,
    hubId: actual?.hubId ?? ev.hubId ?? null,
    eventIdApertura: actual?.eventIdApertura ?? null,
    estado: 'CERRADA',
    cerradaEn: ev.ocurridoEn,
    motivoCierre: ev.motivoCierre ?? null,
  };
}

// ---------------------------------------------------------------- colección notificaciones

export function documentoNotificacion(ev, meta = {}) {
  return {
    _id: ev.eventId,
    shipmentId: ev.shipmentId,
    plantilla: ev.plantilla,
    canal: ev.canal,
    mensaje: ev.mensaje,
    opciones: ev.opciones ?? [],
    causaEventId: ev.causaEventId,
    causaTopic: ev.causaTopic ?? null,
    motivo: ev.motivo ?? null,
    regla: ev.regla ?? null,
    intento: ev.intento ?? null,
    ocurridoEn: ev.ocurridoEn,
    kafkaTs: meta.kafkaTs ?? null,
  };
}
