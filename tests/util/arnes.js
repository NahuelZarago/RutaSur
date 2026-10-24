// Arnés de pruebas: reproduce la tubería completa en memoria, sin Kafka ni MongoDB.
//
//  · Un "log" por topic con 3 particiones (partición = hash(clave) % 3), como Kafka.
//  · Cada consumidor lleva sus offsets por partición, su conjunto de eventos procesados
//    (eventos_procesados) y envía a la DLQ lo que no pasa la validación.
//  · Los servicios usan exactamente las mismas funciones de dominio que en producción.
import { decodificarMensaje } from '../../shared/esquemas.js';
import { HEADER_RELOJ, SUSCRIPCIONES, GRUPOS, TOPICS } from '../../shared/eventos.js';
import { hashTexto } from '../../shared/rng.js';
import { reducirAlerta, reducirEnvio, idAlerta, documentoNotificacion } from '../../shared/dominio/proyeccion.js';
import { decidirNotificacion } from '../../shared/dominio/notificaciones.js';
import { eventoReprogramacion } from '../../shared/dominio/reprogramacion.js';
import { evaluar, procesarEventoMonitor, requiereEvaluacion } from '../../shared/dominio/sla.js';
import { aplicarJoinSla, aplicarTiemposHub, idMetrica, idMetricaTiempo, incrementosPorEstado, participaEnJoinSla } from '../../shared/dominio/analitica.js';
import { MIN } from '../../shared/tiempo.js';

const PARTICIONES = 3;

export class Bus {
  constructor() {
    this.logs = new Map(); // topic → [[mensajes p0], [p1], [p2]]
    this.ts = 1_700_000_000_000;
  }
  publicar(topic, key, value, headers = {}) {
    if (!this.logs.has(topic)) this.logs.set(topic, Array.from({ length: PARTICIONES }, () => []));
    const particion = hashTexto(key) % PARTICIONES;
    const log = this.logs.get(topic)[particion];
    log.push({ key, value, headers, offset: log.length, timestamp: ++this.ts, partition: particion });
  }
  publicarEvento(topic, evento) {
    this.publicar(topic, evento.shipmentId, JSON.stringify(evento));
  }
  particiones(topic) {
    return this.logs.get(topic) ?? Array.from({ length: PARTICIONES }, () => []);
  }
  todos(topic) {
    return this.particiones(topic).flat();
  }
}

/** Consumidor genérico con offsets, idempotencia por eventId y DLQ. */
class Consumidor {
  constructor(bus, nombre, topics, manejar) {
    Object.assign(this, { bus, nombre, topics, manejar });
    this.offsets = new Map();
    this.procesados = new Set();
    this.dlq = [];
    this.efectosPorEvento = new Map();
  }
  /** Consume todo lo pendiente. `orden` permite intercalar particiones/topics de otra forma. */
  consumir(orden = 'secuencial') {
    const colas = [];
    for (const topic of this.topics) {
      this.bus.particiones(topic).forEach((log, p) => {
        const clave = `${topic}:${p}`;
        const desde = this.offsets.get(clave) ?? 0;
        if (desde < log.length) colas.push({ topic, p, clave, mensajes: log.slice(desde) });
      });
    }
    if (orden === 'secuencial') {
      for (const c of colas) for (const m of c.mensajes) this.procesar(c, m);
    } else {
      // Intercalado pseudoaleatorio entre colas, respetando el orden dentro de cada partición.
      let semilla = orden;
      const azar = () => ((semilla = (semilla * 1103515245 + 12345) % 2147483648) / 2147483648);
      const activas = colas.map((c) => ({ ...c, i: 0 }));
      while (activas.length) {
        const k = Math.floor(azar() * activas.length);
        const c = activas[k];
        this.procesar(c, c.mensajes[c.i++]);
        if (c.i >= c.mensajes.length) activas.splice(k, 1);
      }
    }
  }
  procesar(cola, m) {
    const d = decodificarMensaje(cola.topic, m.key, m.value);
    if (!d.ok) this.dlq.push({ topic: cola.topic, offset: m.offset, motivo: d.errores.join('; ') });
    else if (!this.procesados.has(d.evento.eventId)) {
      this.manejar(d.evento, { topic: cola.topic, partition: cola.p, offset: m.offset, kafkaTs: m.timestamp, headers: m.headers });
      this.procesados.add(d.evento.eventId);
    }
    this.offsets.set(cola.clave, m.offset + 1);
  }
}

export function crearProyector(bus) {
  const vistas = { envios: new Map(), alertas: new Map(), notificaciones: new Map() };
  const c = new Consumidor(bus, 'proyector', SUSCRIPCIONES[GRUPOS.PROYECTOR], (ev, meta) => {
    const { envio } = reducirEnvio(vistas.envios.get(ev.shipmentId) ?? null, meta.topic, ev, meta);
    vistas.envios.set(ev.shipmentId, envio);
    if (meta.topic === TOPICS.ALERTAS) vistas.alertas.set(idAlerta(ev), reducirAlerta(vistas.alertas.get(idAlerta(ev)) ?? null, ev));
    if (meta.topic === TOPICS.NOTIFICACIONES) vistas.notificaciones.set(ev.eventId, documentoNotificacion(ev, meta));
  });
  c.vistas = vistas;
  return c;
}

export function crearNotificaciones(bus) {
  return new Consumidor(bus, 'notificaciones', SUSCRIPCIONES[GRUPOS.NOTIFICACIONES], (ev, meta) => {
    const n = decidirNotificacion(meta.topic, ev);
    if (n) bus.publicarEvento(TOPICS.NOTIFICACIONES, n);
  });
}

export function crearReprogramacion(bus) {
  return new Consumidor(bus, 'reprogramacion', SUSCRIPCIONES[GRUPOS.REPROGRAMACION], (ev) => {
    if (ev.tipo === 'ENTREGA_FALLIDA') bus.publicarEvento(TOPICS.REPROGRAMACIONES, eventoReprogramacion(ev));
  });
}

export function crearMonitor(bus, estadoInicial = null) {
  const tabla = new Map(estadoInicial?.tabla ?? []);
  let relojMs = estadoInicial?.relojMs ?? 0;
  const c = new Consumidor(bus, 'monitor-sla', SUSCRIPCIONES[GRUPOS.MONITOR_SLA], (ev, meta) => {
    relojMs = Math.max(relojMs, Date.parse(ev.ocurridoEn));
    const r = procesarEventoMonitor(tabla.get(ev.shipmentId), meta.topic, ev, relojMs);
    r.emitir.forEach((e) => bus.publicarEvento(TOPICS.ALERTAS, e));
    tabla.set(r.st._id, r.st);
  });
  if (estadoInicial) {
    c.offsets = new Map(estadoInicial.offsets);
    c.procesados = new Set(estadoInicial.procesados);
  }
  c.tick = () => {
    for (const st of tabla.values()) {
      if (!requiereEvaluacion(st)) continue;
      const r = evaluar(st, relojMs);
      r.emitir.forEach((e) => bus.publicarEvento(TOPICS.ALERTAS, e));
      tabla.set(r.st._id, r.st);
    }
  };
  /** Lo que el servicio real respalda en MongoDB y recupera al reiniciar. */
  c.respaldo = () =>
    JSON.parse(JSON.stringify({ tabla: [...tabla], relojMs, offsets: [...c.offsets], procesados: [...c.procesados] }));
  c.tabla = tabla;
  c.reloj = () => relojMs;
  return c;
}

export function crearAnalitica(bus) {
  const metricas = new Map();
  const joins = new Map();
  const c = new Consumidor(bus, 'analitica', SUSCRIPCIONES[GRUPOS.ANALITICA], (ev, meta) => {
    const incs = meta.topic === TOPICS.ESTADOS ? incrementosPorEstado(ev) : [];
    if (participaEnJoinSla(meta.topic, ev)) {
      const r = aplicarJoinSla(joins.get(ev.shipmentId) ?? null, meta.topic, ev);
      joins.set(ev.shipmentId, r.join);
      incs.push(...r.incrementos);
    }
    for (const { clave, inc } of incs) {
      const id = idMetrica(clave);
      const doc = metricas.get(id) ?? { _id: id, ...clave };
      for (const [campo, n] of Object.entries(inc)) {
        const [a, b] = campo.split('.');
        doc[a] ??= {};
        doc[a][b] = (doc[a][b] ?? 0) + n;
      }
      metricas.set(id, doc);
    }
  });
  c.metricas = metricas;
  return c;
}

export function crearTiemposHub(bus) {
  const pendientes = new Map();
  const metricas = new Map();
  const c = new Consumidor(bus, 'tiempos', SUSCRIPCIONES[GRUPOS.TIEMPOS_HUB], (ev) => {
    if (ev.tipo !== 'INGRESADO_HUB' && ev.tipo !== 'EN_REPARTO') return;
    const r = aplicarTiemposHub(pendientes.get(ev.shipmentId) ?? null, ev);
    pendientes.set(ev.shipmentId, r.pendiente);
    if (r.incremento) {
      const id = idMetricaTiempo(r.incremento.clave);
      const doc = metricas.get(id) ?? { _id: id, ...r.incremento.clave, sumaMinutos: 0, cantidad: 0 };
      doc.sumaMinutos += r.incremento.inc.sumaMinutos;
      doc.cantidad += r.incremento.inc.cantidad;
      metricas.set(id, doc);
    }
  });
  c.metricas = metricas;
  return c;
}

/**
 * Corre el motor del simulador durante `duracionMs` de tiempo simulado, publicando en el bus y
 * consumiendo con todos los servicios cada `pasoMs`.
 */
export function correr({ motor, bus, servicios, desdeMs, duracionMs, pasoMs = 5 * MIN, alPaso }) {
  for (let t = desdeMs; t <= desdeMs + duracionMs; t += pasoMs) {
    for (const m of motor.avanzarHasta(t)) bus.publicar(m.topic, m.key, m.value, m.headers);
    for (const s of servicios) {
      s.consumir();
      s.tick?.();
    }
    // segunda vuelta: los eventos derivados (alertas, notificaciones, reprogramaciones)
    for (const s of servicios) s.consumir();
    alPaso?.(t);
  }
}

export { HEADER_RELOJ };
