// Pruebas unitarias de las reglas de negocio (sin infraestructura).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { decodificarMensaje, validarEvento } from '../shared/esquemas.js';
import { TOPICS } from '../shared/eventos.js';
import { fechaLocal, franjaDe, horaLocal, siguienteDiaHabil, HORA, MIN } from '../shared/tiempo.js';
import { aplicarEstado, aplicarReprogramacion, reducirAlerta } from '../shared/dominio/proyeccion.js';
import { calcularProximoIntento, decidirReprogramacion, eventoReprogramacion } from '../shared/dominio/reprogramacion.js';
import { decidirNotificacion } from '../shared/dominio/notificaciones.js';
import { estadoMonitorVacio, evaluar, procesarEventoMonitor } from '../shared/dominio/sla.js';
import { aplicarJoinSla, aplicarTiemposHub, incrementosPorEstado } from '../shared/dominio/analitica.js';

const L = (s) => `${s}-03:00`; // hora local de Argentina
const ev = (tipo, secuencia, extra = {}) => ({
  eventId: `RS-T-${secuencia}`,
  shipmentId: 'RS-T',
  tipo,
  ocurridoEn: new Date(L('2026-11-02T10:00:00')).toISOString(),
  secuencia,
  hubId: 'HUB-SUR',
  zonaId: 'Z-S1',
  repartidorId: null,
  version: 1,
  ...extra,
});

// ------------------------------------------------------------------ tiempo
test('tiempo: franjas, días hábiles y fecha local (UTC-3)', () => {
  assert.equal(fechaLocal('2026-11-03T01:30:00Z'), '2026-11-02'); // 22:30 del lunes en Argentina
  assert.equal(horaLocal(L('2026-11-02T14:30:00')), 14.5);
  assert.equal(franjaDe(L('2026-11-02T12:59:00')), 'MANANA');
  assert.equal(franjaDe(L('2026-11-02T13:00:00')), 'TARDE');
  // viernes → lunes
  assert.equal(fechaLocal(siguienteDiaHabil(L('2026-11-06T18:00:00'))), '2026-11-09');
});

// ------------------------------------------------------------------ esquemas
test('esquemas: acepta un evento válido y rechaza los malformados con motivo', () => {
  const ok = ev('ENTREGA_FALLIDA', 6, { intento: 1, motivo: 'AUSENTE', franja: 'MANANA' });
  assert.deepEqual(validarEvento(TOPICS.ESTADOS, ok), []);
  assert.match(validarEvento(TOPICS.ESTADOS, { ...ok, motivo: undefined }).join(), /motivo/);
  assert.match(validarEvento(TOPICS.ESTADOS, { ...ok, tipo: 'TELETRANSPORTADO' }).join(), /desconocido/);
  assert.match(validarEvento(TOPICS.ESTADOS, { ...ok, secuencia: 'tres' }).join(), /secuencia/);
  assert.match(validarEvento(TOPICS.ESTADOS, { ...ok, version: 2 }).join(), /version/);
  const roto = decodificarMensaje(TOPICS.ESTADOS, 'RS-T', '{"eventId":"x"');
  assert.equal(roto.ok, false);
  assert.match(roto.errores[0], /JSON inválido/);
  const claveDistinta = decodificarMensaje(TOPICS.ESTADOS, 'OTRO', JSON.stringify(ok));
  assert.match(claveDistinta.errores[0], /clave/);
});

// ------------------------------------------------------------------ CU1 proyección
test('CU1: un duplicado o un evento fuera de orden no retroceden el estado ni repiten historial', () => {
  let { envio } = aplicarEstado(null, ev('CREADO', 1));
  ({ envio } = aplicarEstado(envio, ev('CLASIFICADO', 3, { repartidorId: 'R-079' })));
  const fueraDeOrden = aplicarEstado(envio, ev('INGRESADO_HUB', 2));
  assert.equal(fueraDeOrden.aplicado, false);
  assert.equal(fueraDeOrden.envio.estado, 'CLASIFICADO');
  const duplicado = aplicarEstado(fueraDeOrden.envio, ev('CLASIFICADO', 3));
  assert.equal(duplicado.aplicado, false);
  assert.equal(duplicado.envio.historial.length, 2);
  assert.equal(duplicado.envio.descartadosFueraDeOrden, 2);
});

test('CU1: el estado visible refleja la resolución del fallo', () => {
  let { envio } = aplicarEstado(null, ev('ENTREGA_FALLIDA', 5, { intento: 1, motivo: 'AUSENTE', franja: 'MANANA' }));
  assert.equal(envio.estadoVisible, 'ENTREGA_FALLIDA');
  const fallo = ev('ENTREGA_FALLIDA', 5, { intento: 1, motivo: 'AUSENTE', franja: 'MANANA' });
  ({ envio } = aplicarReprogramacion(envio, eventoReprogramacion(fallo)));
  assert.equal(envio.estadoVisible, 'REPROGRAMADO');
});

test('CU1: una alerta cerrada conserva causa y prioridad', () => {
  const apertura = { eventId: 'a', shipmentId: 'RS-T', tipo: 'ALERTA_ABIERTA', regla: 'SIN_SALIDA', prioridad: 'ALTA', causa: 'x', ocurridoEn: '2026-11-02T13:00:00.000Z' };
  const cierre = { eventId: 'c', shipmentId: 'RS-T', tipo: 'ALERTA_CERRADA', regla: 'SIN_SALIDA', ocurridoEn: '2026-11-02T15:00:00.000Z' };
  const doc = reducirAlerta(reducirAlerta(null, apertura), cierre);
  assert.equal(doc.estado, 'CERRADA');
  assert.equal(doc.prioridadOrden, 2);
  assert.equal(doc.causa, 'x');
});

// ------------------------------------------------------------------ CU2 reprogramación
test('CU2: cada motivo aplica su regla de reprogramación', () => {
  const fallo = (motivo, intento = 1, hora = '2026-11-06T10:30:00') =>
    decidirReprogramacion({ motivo, intento, ocurridoEn: new Date(L(hora)).toISOString(), franja: franjaDe(L(hora)) });
  const ausente = fallo('AUSENTE');
  assert.equal(ausente.tipo, 'REPROGRAMADO');
  assert.deepEqual(
    { fecha: ausente.proximoIntento.fecha, franja: ausente.proximoIntento.franja },
    { fecha: '2026-11-09', franja: 'TARDE' } // viernes a la mañana → lunes a la tarde
  );
  assert.equal(fallo('AUSENTE', 1, '2026-11-02T15:00:00').proximoIntento.franja, 'MANANA');
  assert.equal(fallo('DIRECCION_INCORRECTA').tipo, 'RETENIDO');
  assert.equal(fallo('RECHAZADO').tipo, 'DEVOLUCION');
  assert.equal(fallo('ZONA_INACCESIBLE').tipo, 'RETENIDO');
  assert.equal(fallo('AUSENTE', 3).tipo, 'DEVOLUCION');
  assert.equal(fallo('AUSENTE', 3).regla, 'TERCER_FALLO');
  assert.equal(calcularProximoIntento(L('2026-11-02T09:00:00'), 'MANANA').desde, new Date(L('2026-11-03T13:00:00')).toISOString());
});

test('CU2: los eventos derivados tienen eventId determinista', () => {
  const fallo = ev('ENTREGA_FALLIDA', 5, { intento: 1, motivo: 'AUSENTE', franja: 'MANANA' });
  assert.equal(eventoReprogramacion(fallo).eventId, eventoReprogramacion(fallo).eventId);
  assert.equal(decidirNotificacion(TOPICS.ESTADOS, fallo).eventId, `notif:${fallo.eventId}`);
  assert.equal(decidirNotificacion(TOPICS.ESTADOS, ev('ENTREGADO', 5, { intento: 1 })), null);
  const alertaMedia = { tipo: 'ALERTA_ABIERTA', prioridad: 'MEDIA', regla: 'SIN_INGRESO', eventId: 'a', shipmentId: 'RS-T', ocurridoEn: fallo.ocurridoEn };
  assert.equal(decidirNotificacion(TOPICS.ALERTAS, alertaMedia), null);
  assert.equal(decidirNotificacion(TOPICS.ALERTAS, { ...alertaMedia, prioridad: 'ALTA', regla: 'SIN_SALIDA' }).plantilla, 'RIESGO_DEMORA');
});

// ------------------------------------------------------------------ CU3 SLA
const promesa = (venceLocal) => ({ eventId: 'RS-T-P1', shipmentId: 'RS-T', tipo: 'PROMESA', venceEn: new Date(L(venceLocal)).toISOString(), secuencia: 1, ocurridoEn: '2026-11-02T10:00:00.000Z', version: 1 });
const reglas = (r) => r.emitir.map((e) => `${e.regla}:${e.prioridad ?? 'cierre'}`);

test('CU3: cada una de las cuatro reglas genera su alerta con la prioridad definida', () => {
  const vence = '2026-11-03T20:00:00';
  const t = (local) => Date.parse(L(local));
  let r = procesarEventoMonitor(null, TOPICS.PROMESAS, promesa(vence), t('2026-11-02T10:00:00'));
  r = procesarEventoMonitor(r.st, TOPICS.ESTADOS, ev('CREADO', 1), t('2026-11-02T10:00:00'));
  assert.deepEqual(reglas(r), []);
  assert.deepEqual(reglas(evaluar(r.st, t('2026-11-03T10:30:00'))), ['SIN_INGRESO:MEDIA']);
  assert.deepEqual(reglas(evaluar(r.st, t('2026-11-03T16:30:00'))), ['SIN_INGRESO:MEDIA', 'SIN_SALIDA:ALTA']);
  assert.deepEqual(reglas(evaluar(r.st, t('2026-11-03T20:00:00'))), ['VENCIDO:CRITICA']);
  const falloHoy = ev('ENTREGA_FALLIDA', 2, { intento: 1, motivo: 'AUSENTE', ocurridoEn: new Date(L('2026-11-03T09:00:00')).toISOString() });
  const f = procesarEventoMonitor(r.st, TOPICS.ESTADOS, falloHoy, t('2026-11-03T09:00:00'));
  assert.deepEqual(reglas(f), ['FALLO_DIA_LIMITE:ALTA']);
});

test('CU3: una regla no alerta dos veces y la entrega cierra las alertas abiertas', () => {
  const t = (local) => Date.parse(L(local));
  let r = procesarEventoMonitor(null, TOPICS.PROMESAS, promesa('2026-11-02T20:00:00'), t('2026-11-02T10:00:00'));
  r = procesarEventoMonitor(r.st, TOPICS.ESTADOS, ev('CLASIFICADO', 3), t('2026-11-02T17:00:00'));
  assert.deepEqual(reglas(r), ['SIN_SALIDA:ALTA']);
  const otra = evaluar(r.st, t('2026-11-02T17:30:00'));
  assert.deepEqual(reglas(otra), []);
  const entregado = ev('ENTREGADO', 5, { intento: 1, ocurridoEn: new Date(L('2026-11-02T18:00:00')).toISOString() });
  const c = procesarEventoMonitor(r.st, TOPICS.ESTADOS, entregado, t('2026-11-02T18:00:00'));
  assert.deepEqual(reglas(c), ['SIN_SALIDA:cierre']);
  assert.deepEqual(reglas(evaluar(c.st, t('2026-11-02T21:00:00'))), []);
});

test('CU3: un envío que sale del circuito (retenido/devolución) no alerta como vencido', () => {
  const t = (local) => Date.parse(L(local));
  let r = procesarEventoMonitor(null, TOPICS.PROMESAS, promesa('2026-11-04T20:00:00'), t('2026-11-02T10:00:00'));
  const fallo = ev('ENTREGA_FALLIDA', 5, { intento: 1, motivo: 'RECHAZADO', ocurridoEn: new Date(L('2026-11-02T11:00:00')).toISOString() });
  r = procesarEventoMonitor(r.st, TOPICS.ESTADOS, fallo, t('2026-11-02T11:00:00'));
  assert.equal(r.st.fueraDeCircuito, true);
  assert.deepEqual(reglas(evaluar(r.st, t('2026-11-05T10:00:00'))), []);
});

test('CU3: tras reiniciar desde el respaldo, no se repiten alertas', () => {
  const t = (local) => Date.parse(L(local));
  let r = procesarEventoMonitor(null, TOPICS.PROMESAS, promesa('2026-11-02T20:00:00'), t('2026-11-02T10:00:00'));
  r = evaluar(procesarEventoMonitor(r.st, TOPICS.ESTADOS, ev('CREADO', 1), t('2026-11-02T10:00:00')).st, t('2026-11-02T17:00:00'));
  const respaldo = JSON.parse(JSON.stringify(r.st)); // lo que queda en monitor_sla_estado
  assert.deepEqual(reglas(evaluar(respaldo, t('2026-11-02T17:01:00'))), []);
  assert.equal(estadoMonitorVacio('x').alertas.SIN_INGRESO, undefined);
});

// ------------------------------------------------------------------ CU4 analítica
test('CU4: incrementos por estado y join de SLA simétrico', () => {
  const entregado = ev('ENTREGADO', 5, { intento: 1, franja: 'TARDE', ocurridoEn: new Date(L('2026-11-03T15:00:00')).toISOString() });
  assert.deepEqual(incrementosPorEstado(entregado)[0].inc, { 'entregas.total': 1, 'primerIntento.total': 1, 'primerIntento.exitosos': 1 });
  const fallo2 = ev('ENTREGA_FALLIDA', 7, { intento: 2, motivo: 'AUSENTE', franja: 'TARDE' });
  assert.deepEqual(incrementosPorEstado(fallo2)[0].inc, { 'fallos.total': 1, 'fallos.AUSENTE': 1 });

  const p = promesa('2026-11-03T20:00:00');
  // primero la entrega, después la promesa (como puede pasar al reprocesar)
  const a = aplicarJoinSla(null, TOPICS.ESTADOS, entregado);
  assert.equal(a.incrementos.length, 0);
  const b = aplicarJoinSla(a.join, TOPICS.PROMESAS, p);
  assert.deepEqual(b.incrementos[0].inc, { 'entregas.conPromesa': 1, 'entregas.enPlazo': 1 });
  assert.equal(b.incrementos[0].clave.fecha, '2026-11-03');
  assert.equal(aplicarJoinSla(b.join, TOPICS.PROMESAS, p).incrementos.length, 0);
});

test('CU4: tiempo entre ingreso al hub y salida a reparto', () => {
  const ingreso = ev('INGRESADO_HUB', 2, { ocurridoEn: new Date(L('2026-11-02T10:00:00')).toISOString() });
  const salida = ev('EN_REPARTO', 4, { intento: 1, ocurridoEn: new Date(L('2026-11-02T13:30:00')).toISOString() });
  const a = aplicarTiemposHub(null, salida); // llega antes la salida
  const b = aplicarTiemposHub(a.pendiente, ingreso);
  assert.deepEqual(b.incremento, { clave: { fecha: '2026-11-02', hubId: 'HUB-SUR' }, inc: { sumaMinutos: 210, cantidad: 1 } });
});

test('el catálogo de la web es idéntico al de los servicios', () => {
  const a = readFileSync(new URL('../shared/catalogo.json', import.meta.url), 'utf8');
  const b = readFileSync(new URL('../web/lib/catalogo.json', import.meta.url), 'utf8');
  assert.deepEqual(JSON.parse(b), JSON.parse(a));
});

test('unidades de tiempo', () => {
  assert.equal(HORA, 60 * MIN);
});
