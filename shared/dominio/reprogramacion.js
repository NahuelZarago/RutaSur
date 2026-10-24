// CU2 — Reglas de reprogramación tras una entrega fallida.
//
//  · AUSENTE: nuevo intento al día hábil siguiente, en la franja opuesta a la del fallo.
//  · DIRECCION_INCORRECTA: retenido hasta una corrección; no se reprograma solo.
//  · RECHAZADO: devolución al comercio.
//  · ZONA_INACCESIBLE: el enunciado no fija regla; se adopta "retenido en hub hasta que la zona
//    vuelva a ser accesible" (requiere intervención, igual que una dirección incorrecta).
//  · Tercer intento fallido, cualquiera sea el motivo: devolución al comercio.
import { VERSION_ESQUEMA } from '../eventos.js';
import { aHoraLocal, fechaLocal, FRANJAS, franjaDe, franjaOpuesta, iso, siguienteDiaHabil } from '../tiempo.js';

export const MAX_INTENTOS = 3;

export function calcularProximoIntento(falloEn, franjaFallo) {
  const falloMs = typeof falloEn === 'number' ? falloEn : Date.parse(falloEn);
  const franja = franjaOpuesta(franjaFallo ?? franjaDe(falloMs));
  const dia = siguienteDiaHabil(falloMs);
  return { fecha: fechaLocal(dia), franja, desde: iso(aHoraLocal(dia, FRANJAS[franja].desde)) };
}

/** Un fallo por ausencia vuelve al circuito; los demás motivos, o un tercer fallo, lo sacan de él. */
export function saleDelCircuito(motivo, intento) {
  return intento >= MAX_INTENTOS || motivo !== 'AUSENTE';
}

export function decidirReprogramacion(fallo) {
  if (fallo.intento >= MAX_INTENTOS) {
    return {
      tipo: 'DEVOLUCION',
      regla: 'TERCER_FALLO',
      detalle: `Se agotaron los ${MAX_INTENTOS} intentos de entrega: vuelve al comercio.`,
    };
  }
  switch (fallo.motivo) {
    case 'AUSENTE': {
      const proximoIntento = calcularProximoIntento(fallo.ocurridoEn, fallo.franja);
      return {
        tipo: 'REPROGRAMADO',
        regla: 'AUSENTE',
        proximoIntento,
        detalle: `Nuevo intento el ${proximoIntento.fecha} en la franja ${FRANJAS[proximoIntento.franja].nombre.toLowerCase()}.`,
      };
    }
    case 'DIRECCION_INCORRECTA':
      return {
        tipo: 'RETENIDO',
        regla: 'DIRECCION_INCORRECTA',
        detalle: 'Retenido en el hub hasta que se corrija la dirección.',
      };
    case 'RECHAZADO':
      return { tipo: 'DEVOLUCION', regla: 'RECHAZADO', detalle: 'El destinatario rechazó el paquete: vuelve al comercio.' };
    case 'ZONA_INACCESIBLE':
      return {
        tipo: 'RETENIDO',
        regla: 'ZONA_INACCESIBLE',
        detalle: 'Retenido en el hub hasta que la zona vuelva a ser accesible.',
      };
    default:
      throw new Error(`Motivo de fallo desconocido: ${fallo.motivo}`);
  }
}

/** Evento para envios.reprogramaciones. El eventId es determinista: un reintento no duplica. */
export function eventoReprogramacion(fallo) {
  const decision = decidirReprogramacion(fallo);
  return {
    eventId: `reprog:${fallo.eventId}`,
    shipmentId: fallo.shipmentId,
    tipo: decision.tipo,
    regla: decision.regla,
    detalle: decision.detalle,
    motivo: fallo.motivo,
    intento: fallo.intento,
    causaEventId: fallo.eventId,
    ...(decision.proximoIntento && { proximoIntento: decision.proximoIntento }),
    hubId: fallo.hubId,
    zonaId: fallo.zonaId,
    ocurridoEn: fallo.ocurridoEn,
    version: VERSION_ESQUEMA,
  };
}
