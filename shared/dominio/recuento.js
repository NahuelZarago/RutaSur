// Recuento manual de los tres indicadores directamente desde los eventos crudos de Kafka.
// Es un camino de cálculo independiente del servicio de analítica (no usa $inc ni el join
// incremental): sirve para verificar el criterio "los tres indicadores coinciden con un recuento
// manual sobre una corrida con semilla fija".
import { fechaLocal, franjaDe } from '../tiempo.js';

const sumar = (obj, clave, campo, n = 1) => {
  obj[clave] ??= {};
  obj[clave][campo] = (obj[clave][campo] ?? 0) + n;
};

/**
 * @param {object[]} estados   eventos válidos de envios.estados (pueden tener duplicados)
 * @param {object[]} promesas  eventos válidos de envios.promesas
 */
export function recontarIndicadores(estados, promesas) {
  const vistos = new Set();
  const unicos = estados.filter((e) => (vistos.has(e.eventId) ? false : vistos.add(e.eventId)));

  const resultado = { primerIntento: {}, motivos: {}, sla: {} };
  const primeraEntrega = new Map();

  for (const e of unicos) {
    const fecha = fechaLocal(e.ocurridoEn);
    const franja = e.franja ?? franjaDe(e.ocurridoEn);
    const celda = `${fecha}|${e.zonaId}|${franja}`;
    if (e.tipo === 'ENTREGA_FALLIDA') {
      sumar(resultado.motivos, e.zonaId, e.motivo);
      if (e.intento === 1) sumar(resultado.primerIntento, celda, 'total');
    }
    if (e.tipo === 'ENTREGADO') {
      if (e.intento === 1) {
        sumar(resultado.primerIntento, celda, 'total');
        sumar(resultado.primerIntento, celda, 'exitosos');
      }
      if (!primeraEntrega.has(e.shipmentId)) primeraEntrega.set(e.shipmentId, e);
    }
  }

  const ultimaPromesa = new Map();
  for (const p of promesas) {
    const previa = ultimaPromesa.get(p.shipmentId);
    if (!previa || p.secuencia > previa.secuencia) ultimaPromesa.set(p.shipmentId, p);
  }
  for (const [shipmentId, entrega] of primeraEntrega) {
    const promesa = ultimaPromesa.get(shipmentId);
    if (!promesa) continue;
    const clave = `${entrega.hubId}|${fechaLocal(entrega.ocurridoEn)}`;
    sumar(resultado.sla, clave, 'conPromesa');
    sumar(resultado.sla, clave, 'enPlazo', Date.parse(entrega.ocurridoEn) <= Date.parse(promesa.venceEn) ? 1 : 0);
  }
  return normalizar(resultado);
}

/** Convierte documentos de metricas_diarias a la misma forma que recontarIndicadores. */
export function indicadoresDesdeMetricas(documentos) {
  const resultado = { primerIntento: {}, motivos: {}, sla: {} };
  for (const d of documentos) {
    const celda = `${d.fecha}|${d.zonaId}|${d.franja}`;
    if (d.primerIntento?.total) {
      sumar(resultado.primerIntento, celda, 'total', d.primerIntento.total);
      sumar(resultado.primerIntento, celda, 'exitosos', d.primerIntento.exitosos ?? 0);
    }
    for (const [motivo, n] of Object.entries(d.fallos ?? {})) {
      if (motivo !== 'total' && n) sumar(resultado.motivos, d.zonaId, motivo, n);
    }
    if (d.entregas?.conPromesa) {
      const clave = `${d.hubId}|${d.fecha}`;
      sumar(resultado.sla, clave, 'conPromesa', d.entregas.conPromesa);
      sumar(resultado.sla, clave, 'enPlazo', d.entregas.enPlazo ?? 0);
    }
  }
  return normalizar(resultado);
}

function normalizar(r) {
  // exitosos en 0 no se registra en metricas (el $inc solo suma cuando hay éxito); lo igualamos.
  for (const celda of Object.values(r.primerIntento)) celda.exitosos ??= 0;
  for (const celda of Object.values(r.sla)) celda.enPlazo ??= 0;
  return r;
}

/** Lista legible de diferencias entre dos juegos de indicadores (vacía si coinciden). */
export function compararIndicadores(esperado, obtenido) {
  const diferencias = [];
  for (const indicador of ['primerIntento', 'motivos', 'sla']) {
    const claves = new Set([...Object.keys(esperado[indicador]), ...Object.keys(obtenido[indicador])]);
    for (const clave of claves) {
      const a = esperado[indicador][clave] ?? {};
      const b = obtenido[indicador][clave] ?? {};
      for (const campo of new Set([...Object.keys(a), ...Object.keys(b)])) {
        if ((a[campo] ?? 0) !== (b[campo] ?? 0)) {
          diferencias.push(`${indicador}[${clave}].${campo}: recuento=${a[campo] ?? 0} metricas=${b[campo] ?? 0}`);
        }
      }
    }
  }
  return diferencias;
}
