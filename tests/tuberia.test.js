// Prueba de punta a punta en memoria: simulador → "Kafka" → los seis consumidores, con anomalías
// (duplicados, fuera de orden, malformados, demoras de sincronización). Verifica los criterios de
// aceptación que no dependen de la infraestructura.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { TOPICS } from '../shared/eventos.js';
import { decodificarMensaje } from '../shared/esquemas.js';
import { compararIndicadores, indicadoresDesdeMetricas, recontarIndicadores } from '../shared/dominio/recuento.js';
import { DIA, HORA, MIN } from '../shared/tiempo.js';
import { Motor } from '../services/simulador/motor.js';
import { ejecutarEscenario } from '../services/simulador/escenarios.js';
import {
  Bus,
  correr,
  crearAnalitica,
  crearMonitor,
  crearNotificaciones,
  crearProyector,
  crearReprogramacion,
  crearTiemposHub,
} from './util/arnes.js';

const INICIO = Date.parse('2026-11-02T07:00:00-03:00');
const PARAMETROS = { maxEnvios: 400, probDuplicado: 0.04, probFueraDeOrden: 0.04, probMalformado: 0.02 };

function escenarioCompleto() {
  const bus = new Bus();
  const motor = new Motor({ semilla: 2026, inicioMs: INICIO, corrida: 'T', parametros: PARAMETROS });
  const s = {
    proyector: crearProyector(bus),
    notificaciones: crearNotificaciones(bus),
    reprogramacion: crearReprogramacion(bus),
    monitor: crearMonitor(bus),
    analitica: crearAnalitica(bus),
  };
  const servicios = Object.values(s);
  correr({ motor, bus, servicios, desdeMs: INICIO, duracionMs: 5 * DIA, pasoMs: 5 * MIN });
  return { bus, motor, s, servicios };
}

const corrida = escenarioCompleto();

function eventosValidos(bus, topic) {
  return bus
    .todos(topic)
    .map((m) => decodificarMensaje(topic, m.key, m.value))
    .filter((d) => d.ok)
    .map((d) => d.evento);
}

test('ciclo de vida: cada envío refleja su último estado, sin historial repetido ni retrocesos', () => {
  const { bus, s, motor } = corrida;
  const envios = s.proyector.vistas.envios;
  assert.equal(motor.estadisticas.enviosCreados, 400);
  assert.equal([...envios.values()].filter((e) => e.estado).length, 400);

  // Último estado esperado según el log (mayor secuencia por envío)
  const ultimo = new Map();
  for (const e of eventosValidos(bus, TOPICS.ESTADOS)) {
    if (!ultimo.has(e.shipmentId) || e.secuencia > ultimo.get(e.shipmentId).secuencia) ultimo.set(e.shipmentId, e);
  }
  for (const [id, e] of ultimo) {
    const envio = envios.get(id);
    assert.equal(envio.estado, e.tipo, id);
    const ids = envio.historial.map((h) => h.eventId);
    assert.equal(new Set(ids).size, ids.length, `historial sin duplicados (${id})`);
    const secuencias = envio.historial.map((h) => h.secuencia);
    assert.deepEqual(secuencias, [...secuencias].sort((a, b) => a - b), `historial monótono (${id})`);
  }
  assert.ok(motor.estadisticas.duplicados > 0 && motor.estadisticas.fueraDeOrden > 0);
});

test('DLQ: cada consumidor de estados rechaza exactamente los mensajes malformados', () => {
  const { s, motor } = corrida;
  for (const nombre of ['proyector', 'notificaciones', 'reprogramacion', 'monitor', 'analitica']) {
    const dlqEstados = s[nombre].dlq.filter((d) => d.topic === TOPICS.ESTADOS);
    assert.equal(dlqEstados.length, motor.estadisticas.malformados, nombre);
  }
  assert.ok(motor.estadisticas.malformados > 0);
});

test('entrega fallida: exactamente una notificación y una reprogramación por fallo', () => {
  const { bus, s } = corrida;
  const fallos = new Set(eventosValidos(bus, TOPICS.ESTADOS).filter((e) => e.tipo === 'ENTREGA_FALLIDA').map((e) => e.eventId));
  const notifPorFallo = [...s.proyector.vistas.notificaciones.values()].filter((n) => n.causaTopic === TOPICS.ESTADOS);
  assert.equal(notifPorFallo.length, fallos.size);
  assert.deepEqual(new Set(notifPorFallo.map((n) => n.causaEventId)), fallos);

  const reprogramaciones = [...s.proyector.vistas.envios.values()].flatMap((e) => e.reprogramaciones);
  assert.equal(reprogramaciones.length, fallos.size);
  for (const envio of s.proyector.vistas.envios.values()) {
    if (envio.estado === 'ENTREGA_FALLIDA') assert.notEqual(envio.estadoVisible, 'ENTREGA_FALLIDA', envio._id);
    if (envio.intentos >= 3 && envio.estado === 'ENTREGA_FALLIDA') assert.equal(envio.estadoVisible, 'DEVOLUCION');
  }
});

test('riesgo de SLA: alertas de las cuatro reglas, sin repetidas, cerradas al entregar', () => {
  const { bus, s } = corrida;
  const aperturas = eventosValidos(bus, TOPICS.ALERTAS).filter((a) => a.tipo === 'ALERTA_ABIERTA');
  const reglas = new Set(aperturas.map((a) => a.regla));
  assert.deepEqual([...reglas].sort(), ['FALLO_DIA_LIMITE', 'SIN_INGRESO', 'SIN_SALIDA', 'VENCIDO']);
  const claves = aperturas.map((a) => `${a.shipmentId}:${a.regla}`);
  assert.equal(new Set(claves).size, claves.length, 'una misma regla no alerta dos veces');

  for (const alerta of s.proyector.vistas.alertas.values()) {
    const envio = s.proyector.vistas.envios.get(alerta.shipmentId);
    if (envio.estado === 'ENTREGADO') assert.equal(alerta.estado, 'CERRADA', alerta._id);
  }
});

test('analítica: los tres indicadores coinciden con el recuento manual', () => {
  const { bus, s } = corrida;
  const esperado = recontarIndicadores(eventosValidos(bus, TOPICS.ESTADOS), eventosValidos(bus, TOPICS.PROMESAS));
  const obtenido = indicadoresDesdeMetricas([...s.analitica.metricas.values()]);
  assert.deepEqual(compararIndicadores(esperado, obtenido), []);
  assert.ok(Object.keys(esperado.primerIntento).length > 10);
  assert.ok(Object.keys(esperado.sla).length >= 3);
});

test('reprocesamiento: un grupo nuevo completa el histórico del cuarto indicador', () => {
  const { bus } = corrida;
  const tiempos = crearTiemposHub(bus); // se "enciende" con todo el histórico ya en el log
  tiempos.consumir();
  const filas = [...tiempos.metricas.values()];
  const total = filas.reduce((s, f) => s + f.cantidad, 0);
  const salidasPrimerIntento = new Set(
    eventosValidos(bus, TOPICS.ESTADOS).filter((e) => e.tipo === 'EN_REPARTO' && e.intento === 1).map((e) => e.shipmentId)
  );
  assert.equal(total, salidasPrimerIntento.size);
  assert.ok(filas.every((f) => f.sumaMinutos / f.cantidad > 30));
});

test('reconstrucción: borrar las vistas y reprocesar en otro orden deja el mismo resultado', () => {
  const { bus, s } = corrida;
  for (const semilla of [11, 97]) {
    const nuevo = crearProyector(bus);
    nuevo.consumir(semilla); // intercala topics y particiones de otra forma
    for (const vista of ['envios', 'alertas', 'notificaciones']) {
      const a = Object.fromEntries([...s.proyector.vistas[vista]].sort());
      const b = Object.fromEntries([...nuevo.vistas[vista]].sort());
      assert.deepEqual(b, a, `${vista} (orden ${semilla})`);
    }
  }
});

test('recuperación: el monitor reiniciado desde su respaldo no pierde ni repite alertas', () => {
  const bus = new Bus();
  const motor = new Motor({ semilla: 77, inicioMs: INICIO, corrida: 'R', parametros: { maxEnvios: 150 } });
  let monitor = crearMonitor(bus);
  const otros = [crearProyector(bus)];
  const mitad = INICIO + 30 * HORA;
  correr({ motor, bus, servicios: [monitor, ...otros], desdeMs: INICIO, duracionMs: 30 * HORA });
  monitor = crearMonitor(bus, monitor.respaldo()); // "reinicio" con lo persistido
  correr({ motor, bus, servicios: [monitor, ...otros], desdeMs: mitad + 5 * MIN, duracionMs: 3 * DIA });

  const controlBus = new Bus();
  const control = new Motor({ semilla: 77, inicioMs: INICIO, corrida: 'R', parametros: { maxEnvios: 150 } });
  const sinCaida = crearMonitor(controlBus);
  correr({ motor: control, bus: controlBus, servicios: [sinCaida], desdeMs: INICIO, duracionMs: 30 * HORA });
  correr({ motor: control, bus: controlBus, servicios: [sinCaida], desdeMs: mitad + 5 * MIN, duracionMs: 3 * DIA });

  const ids = (b) => b.todos(TOPICS.ALERTAS).map((m) => JSON.parse(m.value).eventId).sort();
  assert.deepEqual(ids(bus), ids(controlBus));
  assert.equal(new Set(ids(bus)).size, ids(bus).length);
});

test('escenario "demorado": la alerta alta aparece antes del vencimiento y se cierra al entregar', () => {
  const bus = new Bus();
  const motor = new Motor({ semilla: 5, inicioMs: INICIO, corrida: 'D' });
  const proyector = crearProyector(bus);
  const monitor = crearMonitor(bus);
  const servicios = [monitor, crearNotificaciones(bus), proyector];
  correr({ motor, bus, servicios, desdeMs: INICIO, duracionMs: 2 * HORA }); // flujo de fondo para el reloj
  const ahora = INICIO + 2 * HORA;
  const { shipmentIds } = ejecutarEscenario(motor, 'demorado', ahora);
  for (const m of motor.tomarSalida()) bus.publicar(m.topic, m.key, m.value, m.headers);
  correr({ motor, bus, servicios, desdeMs: ahora, duracionMs: 12 * HORA, pasoMs: MIN });

  const alertas = [...proyector.vistas.alertas.values()].filter((a) => a.shipmentId === shipmentIds[0]);
  assert.deepEqual(alertas.map((a) => [a.regla, a.prioridad, a.estado]), [['SIN_SALIDA', 'ALTA', 'CERRADA']]);
  assert.ok(Date.parse(alertas[0].abiertaEn) < Date.parse(alertas[0].venceEn));
  assert.equal(proyector.vistas.envios.get(shipmentIds[0]).estado, 'ENTREGADO');
  assert.equal(proyector.vistas.envios.get(shipmentIds[0]).notificaciones.total, 1); // aviso de riesgo
});
