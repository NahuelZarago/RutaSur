// Motor del simulador: lógica pura y determinista (sin Kafka ni reloj real).
//
// Una cola de prioridad ordena acciones por tiempo simulado. avanzarHasta(t) ejecuta todo lo que
// vence hasta t y devuelve los mensajes a publicar. Como cada acción tiene un desempate fijo, el
// resultado depende solo de la semilla y del tiempo simulado, no del ritmo del reloj real: con la
// misma semilla se obtiene exactamente la misma corrida.
import { catalogo, repartidorDeZona, zonaPorId } from '../../shared/catalogo.js';
import { HEADER_RELOJ, TOPICS, VERSION_ESQUEMA } from '../../shared/eventos.js';
import { crearRng } from '../../shared/rng.js';
import { calcularProximoIntento, MAX_INTENTOS } from '../../shared/dominio/reprogramacion.js';
import {
  aHoraLocal,
  DIA,
  esDiaHabil,
  FRANJAS,
  franjaDe,
  HORA,
  horaLocal,
  inicioDiaLocal,
  iso,
  MIN,
  siguienteDiaHabil,
} from '../../shared/tiempo.js';

export const PARAMETROS_POR_DEFECTO = {
  enviosPorMinuto: 20, // por minuto real
  factor: 120, // 1 hora simulada = 30 segundos reales
  probDemoraHub: 0.1,
  probDuplicado: 0.02,
  probFueraDeOrden: 0.02,
  probMalformado: 0.01,
  probDemoraSincronizacion: 0.03,
  distribucionMotivos: { AUSENTE: 0.6, DIRECCION_INCORRECTA: 0.2, RECHAZADO: 0.15, ZONA_INACCESIBLE: 0.05 },
  maxEnvios: 0, // 0 = flujo continuo sin límite
};

const CLASES_MALFORMADO = ['JSON_ROTO', 'SIN_SHIPMENT_ID', 'TIPO_DESCONOCIDO', 'SECUENCIA_INVALIDA', 'MOTIVO_FALTANTE'];

class ColaPrioridad {
  constructor() {
    this.items = [];
  }
  get size() {
    return this.items.length;
  }
  menor(a, b) {
    return a.t < b.t || (a.t === b.t && a.seq < b.seq);
  }
  push(x) {
    const it = this.items;
    it.push(x);
    let i = it.length - 1;
    while (i > 0) {
      const p = (i - 1) >> 1;
      if (!this.menor(it[i], it[p])) break;
      [it[i], it[p]] = [it[p], it[i]];
      i = p;
    }
  }
  peek() {
    return this.items[0];
  }
  pop() {
    const it = this.items;
    const tope = it[0];
    const ultimo = it.pop();
    if (it.length) {
      it[0] = ultimo;
      let i = 0;
      for (;;) {
        const l = 2 * i + 1;
        const r = l + 1;
        let m = i;
        if (l < it.length && this.menor(it[l], it[m])) m = l;
        if (r < it.length && this.menor(it[r], it[m])) m = r;
        if (m === i) break;
        [it[i], it[m]] = [it[m], it[i]];
        i = m;
      }
    }
    return tope;
  }
}

/** Promesa de entrega: creado antes de las 14 h → día hábil siguiente a las 20 h; si no, el subsiguiente. */
export function calcularPromesa(t) {
  const primero = siguienteDiaHabil(t);
  const dia = horaLocal(t) < 14 ? primero : siguienteDiaHabil(primero);
  return aHoraLocal(dia, 20);
}

/** Próxima salida a reparto: inicio de la próxima franja hábil con al menos 30 min de preparación. */
export function proximaSalida(t, rng) {
  const inicio = t + 30 * MIN;
  let dia = inicioDiaLocal(inicio);
  for (let i = 0; i < 10; i++, dia += DIA) {
    if (!esDiaHabil(dia)) continue;
    for (const f of ['MANANA', 'TARDE']) {
      const s = aHoraLocal(dia, FRANJAS[f].desde);
      if (s >= inicio) return s + rng.entre(0, 45) * MIN;
    }
  }
  throw new Error('No se encontró franja de salida');
}

function duracionReparto(t, rng) {
  const fin = aHoraLocal(t, FRANJAS[franjaDe(t)].hasta) - 5 * MIN;
  const d = rng.entre(0.5, 4.5) * HORA;
  return Math.max(10 * MIN, Math.min(d, fin - t));
}

/** Valor del guion de un escenario si está definido (null = "nunca"); si no, el valor por defecto. */
function delGuion(guion, campo, porDefecto) {
  return guion && campo in guion ? guion[campo] : porDefecto();
}

export class Motor {
  constructor({ semilla = 42, inicioMs, corrida = 'DEMO', parametros = {} }) {
    if (!Number.isFinite(inicioMs)) throw new Error('inicioMs es obligatorio');
    this.parametros = { ...PARAMETROS_POR_DEFECTO, ...parametros };
    this.rngFlujo = crearRng(`${semilla}:flujo`);
    this.rngEscenarios = crearRng(`${semilla}:escenarios`);
    this.corrida = corrida;
    this.cola = new ColaPrioridad();
    this.seq = 0;
    this.ahoraMs = inicioMs;
    this.envios = new Map();
    this.contadores = { flujo: 0, escenario: 0, rafaga: 0, malformados: 0 };
    this.estadisticas = { enviosCreados: 0, mensajes: 0, duplicados: 0, fueraDeOrden: 0, malformados: 0, demorasSincronizacion: 0 };
    this.flujoActivo = true;
    this.flujoProgramado = false;
    this.salida = [];
    this.programarFlujo(inicioMs);
  }

  get intervaloFlujoSim() {
    return (60_000 / this.parametros.enviosPorMinuto) * this.parametros.factor;
  }

  get flujoAgotado() {
    return this.parametros.maxEnvios > 0 && this.contadores.flujo >= this.parametros.maxEnvios;
  }

  programar(t, accion) {
    this.cola.push({ t, seq: this.seq++, accion });
  }

  programarFlujo(t) {
    if (this.flujoProgramado || !this.flujoActivo || this.flujoAgotado) return;
    this.flujoProgramado = true;
    this.programar(t, { tipo: 'crearFlujo' });
  }

  detenerFlujo() {
    this.flujoActivo = false;
  }

  reanudarFlujo() {
    this.flujoActivo = true;
    this.programarFlujo(this.ahoraMs);
  }

  /** Ejecuta todas las acciones vencidas hasta simMs y devuelve los mensajes generados, en orden. */
  avanzarHasta(simMs) {
    while (this.cola.size && this.cola.peek().t <= simMs) {
      const { t, accion } = this.cola.pop();
      this.ahoraMs = t;
      this.ejecutar(accion, t);
    }
    this.ahoraMs = Math.max(this.ahoraMs, simMs);
    return this.tomarSalida();
  }

  /** Mensajes generados fuera de avanzarHasta (escenarios disparados a pedido). */
  tomarSalida() {
    const s = this.salida;
    this.salida = [];
    return s;
  }

  get pendientes() {
    return this.cola.size;
  }

  ejecutar(accion, t) {
    switch (accion.tipo) {
      case 'crearFlujo':
        this.flujoProgramado = false;
        if (this.flujoActivo && !this.flujoAgotado) {
          this.crearEnvio(t, { origen: 'flujo' });
          this.programarFlujo(t + this.intervaloFlujoSim);
        }
        break;
      case 'crearRafaga':
        this.crearEnvio(t, { origen: 'rafaga' });
        break;
      case 'paso':
        this.paso(this.envios.get(accion.id), accion.paso, t);
        break;
      case 'publicarDiferido':
        this.entregarEstado(this.envios.get(accion.id), accion.evento, t);
        break;
      default:
        throw new Error(`Acción desconocida: ${accion.tipo}`);
    }
  }

  crearEnvio(t, { origen, guion = null, zonaId = null }) {
    const rng = origen === 'flujo' ? this.rngFlujo : this.rngEscenarios;
    const n = ++this.contadores[origen];
    const prefijo = { flujo: '', escenario: 'E', rafaga: 'R' }[origen];
    const id = `RS-${this.corrida}-${prefijo}${String(n).padStart(origen === 'escenario' ? 3 : 5, '0')}`;
    const zona = zonaId ? zonaPorId(zonaId) : rng.elegir(catalogo.zonas);
    const conAnomalias = origen !== 'escenario';
    const envio = {
      id,
      zona,
      origen,
      rng,
      guion,
      anomalias: conAnomalias,
      secuencia: 0,
      intento: 0,
      franjaIntento: null,
      repartidorId: null,
      retenido: null,
      comercio: rng.elegir(catalogo.comercios),
      demora: null,
    };
    if (conAnomalias && rng.prob(this.parametros.probDemoraHub)) {
      envio.demora = rng.prob(0.35) ? 'INGRESO' : 'SALIDA';
    }
    this.envios.set(id, envio);
    this.estadisticas.enviosCreados++;
    this.paso(envio, 'CREADO', t);
    return id;
  }

  paso(envio, paso, t) {
    const g = envio.guion;
    const rng = envio.rng;
    switch (paso) {
      case 'CREADO': {
        this.emitirEstado(envio, 'CREADO', t, { comercio: envio.comercio, destino: { localidad: envio.zona.nombre } });
        const venceEn = g?.venceEn ? g.venceEn(t) : calcularPromesa(t);
        this.publicar(envio, TOPICS.PROMESAS, {
          eventId: `${envio.id}-P1`,
          shipmentId: envio.id,
          tipo: 'PROMESA',
          ocurridoEn: iso(t),
          venceEn: iso(venceEn),
          secuencia: 1,
          hubId: envio.zona.hubId,
          zonaId: envio.zona.id,
          version: VERSION_ESQUEMA,
        }, t);
        const d = delGuion(g, 'ingreso', () => rng.entre(0.5, 3) * HORA + (envio.demora === 'INGRESO' ? rng.entre(26, 30) * HORA : 0));
        if (d !== null) this.programar(t + d, { tipo: 'paso', id: envio.id, paso: 'INGRESADO_HUB' });
        break;
      }
      case 'INGRESADO_HUB': {
        this.emitirEstado(envio, 'INGRESADO_HUB', t);
        const d = delGuion(g, 'clasificado', () => rng.entre(0.5, 2) * HORA);
        if (d !== null) this.programar(t + d, { tipo: 'paso', id: envio.id, paso: 'CLASIFICADO' });
        break;
      }
      case 'CLASIFICADO': {
        envio.repartidorId = repartidorDeZona(envio.zona.id, rng.entero(0, 12));
        this.emitirEstado(envio, 'CLASIFICADO', t);
        const relativo = delGuion(g, 'salida', () => undefined);
        let salida;
        if (relativo === null) salida = null;
        else if (relativo !== undefined) salida = t + relativo;
        else salida = proximaSalida(t + (envio.demora === 'SALIDA' ? DIA : 0), rng);
        if (salida !== null) this.programar(salida, { tipo: 'paso', id: envio.id, paso: 'EN_REPARTO' });
        break;
      }
      case 'EN_REPARTO': {
        envio.intento++;
        envio.franjaIntento = franjaDe(t);
        this.emitirEstado(envio, 'EN_REPARTO', t, { intento: envio.intento, franja: envio.franjaIntento });
        const d = delGuion(g, 'resultado', () => duracionReparto(t, rng));
        this.programar(t + d, { tipo: 'paso', id: envio.id, paso: 'RESULTADO' });
        break;
      }
      case 'RESULTADO': {
        const resultado = g?.resultados ? g.resultados[envio.intento - 1] ?? 'OK' : this.sortearResultado(envio);
        const extra = { intento: envio.intento, franja: envio.franjaIntento };
        let demora = 0;
        if (envio.anomalias && rng.prob(this.parametros.probDemoraSincronizacion)) {
          demora = rng.entre(10, 40) * MIN;
          this.estadisticas.demorasSincronizacion++;
        }
        if (resultado === 'OK') {
          this.emitirEstado(envio, 'ENTREGADO', t, extra, demora);
          break;
        }
        this.emitirEstado(envio, 'ENTREGA_FALLIDA', t, { ...extra, motivo: resultado }, demora);
        if (resultado === 'AUSENTE' && envio.intento < MAX_INTENTOS) {
          const reintento = delGuion(g, 'reintento', () => undefined);
          const cuando =
            reintento !== undefined && reintento !== null
              ? t + reintento
              : Date.parse(calcularProximoIntento(t, envio.franjaIntento).desde) + rng.entre(0, 60) * MIN;
          this.programar(cuando, { tipo: 'paso', id: envio.id, paso: 'EN_REPARTO' });
        }
        break;
      }
      default:
        throw new Error(`Paso desconocido: ${paso}`);
    }
  }

  sortearResultado(envio) {
    const { zona, franjaIntento, rng } = envio;
    const p = zona.tasaFallo * (franjaIntento === 'MANANA' ? zona.factorManana : 1);
    return rng.prob(p) ? rng.ponderado(this.parametros.distribucionMotivos) : 'OK';
  }

  emitirEstado(envio, tipo, t, extra = {}, demoraPublicacion = 0) {
    envio.secuencia++;
    const evento = {
      eventId: `${envio.id}-${String(envio.secuencia).padStart(2, '0')}`,
      shipmentId: envio.id,
      tipo,
      ocurridoEn: iso(t),
      secuencia: envio.secuencia,
      hubId: envio.zona.hubId,
      zonaId: envio.zona.id,
      repartidorId: envio.repartidorId,
      version: VERSION_ESQUEMA,
      ...extra,
    };
    if (demoraPublicacion > 0) {
      // El repartidor estuvo sin señal: el evento ocurrió en t pero llega más tarde.
      this.programar(t + demoraPublicacion, { tipo: 'publicarDiferido', id: envio.id, evento });
      return;
    }
    this.entregarEstado(envio, evento, t);
  }

  /** Aplica las anomalías de orden: retener un evento y publicarlo después del siguiente. */
  entregarEstado(envio, evento, t) {
    const forzadas = envio.guion?.anomalias ?? {};
    if (envio.retenido) {
      const retenido = envio.retenido;
      envio.retenido = null;
      this.publicarEstado(envio, evento, t);
      this.publicarEstado(envio, retenido, t);
      this.estadisticas.fueraDeOrden++;
      return;
    }
    const retenible = evento.tipo === 'INGRESADO_HUB' || evento.tipo === 'CLASIFICADO';
    if (retenible && ((envio.anomalias && envio.rng.prob(this.parametros.probFueraDeOrden)) || forzadas.fueraDeOrden === evento.tipo)) {
      envio.retenido = evento;
      return;
    }
    this.publicarEstado(envio, evento, t);
  }

  publicarEstado(envio, evento, t) {
    const forzadas = envio.guion?.anomalias ?? {};
    this.publicar(envio, TOPICS.ESTADOS, evento, t);
    if ((envio.anomalias && envio.rng.prob(this.parametros.probDuplicado)) || forzadas.duplicar === evento.tipo) {
      this.publicar(envio, TOPICS.ESTADOS, evento, t);
      this.estadisticas.duplicados++;
    }
    if (envio.anomalias && envio.rng.prob(this.parametros.probMalformado)) {
      this.publicarMalformado(envio.id, evento, t);
    }
  }

  publicar(envio, topic, valor, t) {
    this.salida.push({
      topic,
      key: envio.id,
      value: typeof valor === 'string' ? valor : JSON.stringify(valor),
      headers: { [HEADER_RELOJ]: iso(t) },
    });
    this.estadisticas.mensajes++;
  }

  /** Publica un mensaje que no cumple el esquema (para probar la DLQ). Rota entre cinco clases. */
  publicarMalformado(shipmentId, base, t, claseForzada = null) {
    const k = ++this.contadores.malformados;
    const clase = claseForzada ?? CLASES_MALFORMADO[(k - 1) % CLASES_MALFORMADO.length];
    const eventId = `${shipmentId}-X${k}`;
    let valor;
    switch (clase) {
      case 'JSON_ROTO':
        valor = `{"eventId":"${eventId}","shipmentId":"${shipmentId}","tipo":"EN_REPA`;
        break;
      case 'SIN_SHIPMENT_ID': {
        const { shipmentId: _omitido, ...resto } = base;
        valor = { ...resto, eventId };
        break;
      }
      case 'TIPO_DESCONOCIDO':
        valor = { ...base, eventId, tipo: 'TELETRANSPORTADO' };
        break;
      case 'SECUENCIA_INVALIDA':
        valor = { ...base, eventId, secuencia: 'tres' };
        break;
      case 'MOTIVO_FALTANTE': {
        const { motivo: _m, ...resto } = base;
        valor = { ...resto, eventId, tipo: 'ENTREGA_FALLIDA', intento: base.intento ?? 1 };
        break;
      }
      default:
        throw new Error(`Clase de malformado desconocida: ${clase}`);
    }
    this.salida.push({
      topic: TOPICS.ESTADOS,
      key: shipmentId,
      value: typeof valor === 'string' ? valor : JSON.stringify(valor),
      headers: { [HEADER_RELOJ]: iso(t) },
    });
    this.estadisticas.mensajes++;
    this.estadisticas.malformados++;
    return { eventId, clase };
  }
}
