// Pruebas del simulador: determinismo con semilla, escenarios y anomalías.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { TOPICS } from '../shared/eventos.js';
import { decodificarMensaje } from '../shared/esquemas.js';
import { DIA, HORA, MIN } from '../shared/tiempo.js';
import { Motor } from '../services/simulador/motor.js';
import { ejecutarEscenario } from '../services/simulador/escenarios.js';

const INICIO = Date.parse('2026-11-02T07:00:00-03:00');

function correrMotor(semilla, horas, parametros = {}) {
  const motor = new Motor({ semilla, inicioMs: INICIO, corrida: 'T', parametros });
  const salida = [];
  for (let t = INICIO; t <= INICIO + horas * HORA; t += 7 * MIN) salida.push(...motor.avanzarHasta(t));
  return { motor, salida };
}

test('misma semilla → exactamente la misma corrida, aunque cambie el ritmo de avance', () => {
  const a = correrMotor(7, 30).salida;
  const motor = new Motor({ semilla: 7, inicioMs: INICIO, corrida: 'T' });
  const b = [];
  for (let t = INICIO; t <= INICIO + 30 * HORA; t += 23 * MIN) b.push(...motor.avanzarHasta(t));
  b.push(...motor.avanzarHasta(INICIO + 30 * HORA));
  assert.ok(a.length > 1000);
  // `a` termina en el último múltiplo de 7 min; `b` llega exactamente a 30 h: `a` es prefijo de `b`.
  assert.ok(b.length >= a.length);
  assert.deepEqual(b.slice(0, a.length).map((m) => m.value), a.map((m) => m.value));
  const c = correrMotor(8, 30).salida;
  assert.notDeepEqual(c.map((m) => m.value).slice(0, 50), a.map((m) => m.value).slice(0, 50));
});

test('el flujo continuo respeta volumen, secuencias y produce las anomalías configuradas', () => {
  const { motor, salida } = correrMotor(42, 48, { probDuplicado: 0.05, probFueraDeOrden: 0.05, probMalformado: 0.03 });
  // 20 envíos por minuto real con factor 120 → uno cada 6 minutos simulados → 10 por hora simulada
  // (el avance termina en 47 h 57 min, el múltiplo de 7 min más cercano a 48 h → 480 envíos)
  assert.equal(motor.estadisticas.enviosCreados, 480);
  assert.ok(motor.estadisticas.duplicados > 0);
  assert.ok(motor.estadisticas.fueraDeOrden > 0);
  assert.ok(motor.estadisticas.malformados > 0);
  const invalidos = salida.filter((m) => !decodificarMensaje(m.topic, m.key, m.value).ok);
  assert.equal(invalidos.length, motor.estadisticas.malformados);
  for (const m of salida) assert.ok(m.key.startsWith('RS-T-'), 'la clave es el shipmentId');
});

test('la tasa de fallo varía por zona y los motivos siguen la distribución configurada', () => {
  const { salida } = correrMotor(3, 24 * 6, { probMalformado: 0 });
  const eventos = salida.filter((m) => m.topic === TOPICS.ESTADOS).map((m) => JSON.parse(m.value));
  const fallos = eventos.filter((e) => e.tipo === 'ENTREGA_FALLIDA');
  const porMotivo = {};
  for (const f of fallos) porMotivo[f.motivo] = (porMotivo[f.motivo] ?? 0) + 1;
  assert.ok(fallos.length > 100);
  assert.ok(porMotivo.AUSENTE / fallos.length > 0.45, 'AUSENTE ronda el 60%');
  assert.ok(porMotivo.AUSENTE > porMotivo.DIRECCION_INCORRECTA);
  assert.ok(porMotivo.DIRECCION_INCORRECTA > porMotivo.ZONA_INACCESIBLE);
});

test('escenario "tercer fallo": tres ausencias y ninguna entrega', () => {
  const motor = new Motor({ semilla: 1, inicioMs: INICIO, corrida: 'T' });
  motor.detenerFlujo();
  motor.avanzarHasta(INICIO);
  const { shipmentIds } = ejecutarEscenario(motor, 'tercer-fallo', INICIO);
  const salida = motor.tomarSalida();
  salida.push(...motor.avanzarHasta(INICIO + DIA));
  const tipos = salida.filter((m) => m.topic === TOPICS.ESTADOS && m.key === shipmentIds[0]).map((m) => JSON.parse(m.value).tipo);
  assert.deepEqual(tipos, ['CREADO', 'INGRESADO_HUB', 'CLASIFICADO', 'EN_REPARTO', 'ENTREGA_FALLIDA', 'EN_REPARTO', 'ENTREGA_FALLIDA', 'EN_REPARTO', 'ENTREGA_FALLIDA']);
});

test('escenarios "duplicado" y "fuera de orden" producen exactamente esa anomalía', () => {
  for (const [nombre, esperado] of [
    ['duplicado', ['CREADO', 'INGRESADO_HUB', 'INGRESADO_HUB', 'CLASIFICADO', 'EN_REPARTO', 'ENTREGADO']],
    ['fuera-de-orden', ['CREADO', 'CLASIFICADO', 'INGRESADO_HUB', 'EN_REPARTO', 'ENTREGADO']],
  ]) {
    const motor = new Motor({ semilla: 1, inicioMs: INICIO, corrida: 'T' });
    motor.detenerFlujo();
    motor.avanzarHasta(INICIO);
    const { shipmentIds } = ejecutarEscenario(motor, nombre, INICIO);
    const salida = [...motor.tomarSalida(), ...motor.avanzarHasta(INICIO + 6 * HORA)];
    const tipos = salida.filter((m) => m.topic === TOPICS.ESTADOS && m.key === shipmentIds[0]).map((m) => JSON.parse(m.value).tipo);
    assert.deepEqual(tipos, esperado, nombre);
  }
});

test('escenario "ráfaga": 500 envíos en 5 segundos reales', () => {
  const motor = new Motor({ semilla: 1, inicioMs: INICIO, corrida: 'T' });
  motor.detenerFlujo();
  motor.avanzarHasta(INICIO);
  ejecutarEscenario(motor, 'rafaga', INICIO);
  const salida = motor.avanzarHasta(INICIO + 5_000 * 120);
  assert.equal(salida.filter((m) => m.topic === TOPICS.PROMESAS).length, 500);
});

test('escenario "malformado": un mensaje que no pasa la validación', () => {
  const motor = new Motor({ semilla: 1, inicioMs: INICIO, corrida: 'T' });
  for (let i = 0; i < 5; i++) ejecutarEscenario(motor, 'malformado', INICIO);
  const salida = motor.tomarSalida();
  assert.equal(salida.length, 5);
  const motivos = salida.map((m) => decodificarMensaje(m.topic, m.key, m.value));
  assert.ok(motivos.every((d) => !d.ok));
});
