// CU2/CU3 — Avisos al destinatario. No se envían correos ni SMS reales: se registra la
// notificación publicándola en notificaciones.enviadas.
import { TOPICS, VERSION_ESQUEMA } from '../eventos.js';
import { hashTexto } from '../rng.js';

const MENSAJES_FALLO = {
  AUSENTE: 'No encontramos a nadie en el domicilio. Podés elegir otra franja o retirarlo en sucursal.',
  DIRECCION_INCORRECTA: 'No pudimos ubicar la dirección. Revisala y corregila para que podamos volver a intentarlo.',
  RECHAZADO: 'El paquete fue rechazado en destino. Si fue un error, contactá al comercio.',
  ZONA_INACCESIBLE: 'No pudimos acceder a la zona de entrega. Te avisaremos cuando reprogramemos la visita.',
};

const OPCIONES_FALLO = {
  AUSENTE: ['ELEGIR_NUEVA_FRANJA', 'RETIRAR_EN_SUCURSAL'],
  DIRECCION_INCORRECTA: ['CORREGIR_DIRECCION', 'RETIRAR_EN_SUCURSAL'],
  RECHAZADO: ['CONTACTAR_COMERCIO'],
  ZONA_INACCESIBLE: ['RETIRAR_EN_SUCURSAL', 'ELEGIR_NUEVA_FRANJA'],
};

/** Canal simulado, fijo por envío. */
export function canalPara(shipmentId) {
  return hashTexto(shipmentId) % 2 === 0 ? 'SMS' : 'EMAIL';
}

function base(causa, plantilla, mensaje, opciones, causaTopic) {
  return {
    eventId: `notif:${causa.eventId}`,
    shipmentId: causa.shipmentId,
    tipo: 'NOTIFICACION',
    plantilla,
    canal: canalPara(causa.shipmentId),
    mensaje,
    opciones,
    causaEventId: causa.eventId,
    causaTopic,
    hubId: causa.hubId ?? null,
    zonaId: causa.zonaId ?? null,
    ocurridoEn: causa.ocurridoEn,
    version: VERSION_ESQUEMA,
  };
}

export function notificacionPorFallo(fallo) {
  return {
    ...base(fallo, 'ENTREGA_FALLIDA', MENSAJES_FALLO[fallo.motivo], OPCIONES_FALLO[fallo.motivo], TOPICS.ESTADOS),
    motivo: fallo.motivo,
    intento: fallo.intento,
  };
}

export function notificacionPorAlerta(alerta) {
  const mensaje =
    alerta.regla === 'VENCIDO'
      ? 'Tu envío está demorado. Ya estamos trabajando para entregarlo lo antes posible.'
      : 'Tu envío podría demorarse respecto de la fecha prometida. Te mantendremos al tanto.';
  return {
    ...base(alerta, alerta.regla === 'VENCIDO' ? 'ENVIO_DEMORADO' : 'RIESGO_DEMORA', mensaje, ['VER_TRACKING'], TOPICS.ALERTAS),
    regla: alerta.regla,
  };
}

/**
 * Decide si un evento genera una notificación.
 *  · Toda ENTREGA_FALLIDA notifica al destinatario con opciones para reprogramar.
 *  · Las alertas de SLA notifican solo al abrirse y con prioridad ALTA o CRITICA (una alerta MEDIA
 *    es un aviso interno que todavía tiene margen de rescate).
 */
export function decidirNotificacion(topic, evento) {
  if (topic === TOPICS.ESTADOS && evento.tipo === 'ENTREGA_FALLIDA') return notificacionPorFallo(evento);
  if (topic === TOPICS.ALERTAS && evento.tipo === 'ALERTA_ABIERTA' && ['ALTA', 'CRITICA'].includes(evento.prioridad)) {
    return notificacionPorAlerta(evento);
  }
  return null;
}
