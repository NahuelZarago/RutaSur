// Simulador de envíos: reemplaza a los sistemas de origen (depósito, app del repartidor).
// Publica en envios.estados y envios.promesas. No consume ningún topic.
// Expone una API HTTP mínima (puerto 4000) para el panel de demo.
import http from 'node:http';
import { GRUPOS, SUSCRIPCIONES } from '../../shared/eventos.js';
import { calcularLag, contarDlq, crearKafka, crearProductor } from '../../shared/kafka.js';
import { crearLogger } from '../../shared/log.js';
import { iso } from '../../shared/tiempo.js';
import { ejecutarEscenario, listarEscenarios } from './escenarios.js';
import { Motor } from './motor.js';
import { guardarEstadoPersistido, leerEstadoPersistido, RelojSimulado } from './reloj.js';

const log = crearLogger('simulador');
const env = process.env;

const cfg = {
  semilla: env.SIM_SEMILLA ?? '42',
  inicio: env.SIM_INICIO ?? '2026-11-04T07:00:00-03:00', // miércoles
  // Historia previa que se genera de inmediato al arrancar por primera vez (en horas simuladas):
  // el tablero, la cola de alertas y el reprocesamiento tienen datos desde el primer minuto.
  precargaHoras: Number(env.SIM_PRECARGA_HORAS ?? 48),
  factor: Number(env.SIM_FACTOR ?? 120),
  enviosPorMinuto: Number(env.SIM_ENVIOS_POR_MINUTO ?? 20),
  maxEnvios: Number(env.SIM_MAX_ENVIOS ?? 0),
  puerto: Number(env.SIM_PUERTO ?? 4000),
  archivoEstado: env.SIM_ARCHIVO_ESTADO ?? '/data/simulador.json',
  tickMs: Number(env.SIM_TICK_MS ?? 100),
};

const persistido = leerEstadoPersistido(cfg.archivoEstado);
const corrida = env.SIM_CORRIDA ?? persistido?.corrida ?? Date.now().toString(36).slice(-4).toUpperCase();
const reinicios = persistido ? (persistido.reinicios ?? 0) + 1 : 0;
const reloj = new RelojSimulado({ inicioMs: persistido?.simMs ?? Date.parse(cfg.inicio), factor: cfg.factor });

const precargaMs = persistido ? 0 : cfg.precargaHoras * 3_600_000;
const motor = new Motor({
  semilla: reinicios ? `${cfg.semilla}:r${reinicios}` : cfg.semilla,
  inicioMs: reloj.ahora() - precargaMs, // el primer tick "pone al día" la historia de golpe
  corrida: reinicios ? `${corrida}${reinicios}` : corrida,
  parametros: { factor: cfg.factor, enviosPorMinuto: cfg.enviosPorMinuto, maxEnvios: cfg.maxEnvios },
});

const kafka = crearKafka('simulador');
const productor = crearProductor(kafka);
const admin = kafka.admin();
let pendientes = [];
let enviando = false;
let publicados = 0;

async function publicarPendientes() {
  if (enviando || pendientes.length === 0) return;
  enviando = true;
  const lote = pendientes;
  pendientes = [];
  try {
    const porTopic = new Map();
    for (const m of lote) {
      if (!porTopic.has(m.topic)) porTopic.set(m.topic, []);
      porTopic.get(m.topic).push({ key: m.key, value: m.value, headers: m.headers });
    }
    await productor.sendBatch({
      acks: -1,
      topicMessages: [...porTopic].map(([topic, messages]) => ({ topic, messages })),
    });
    publicados += lote.length;
  } catch (e) {
    log.error('no se pudo publicar el lote; se reintenta', { error: e.message, mensajes: lote.length });
    pendientes = lote.concat(pendientes);
  } finally {
    enviando = false;
  }
}

function tick() {
  const nuevos = motor.avanzarHasta(reloj.ahora());
  if (nuevos.length) pendientes.push(...nuevos);
  publicarPendientes();
}

function estado() {
  return {
    corrida: motor.corrida,
    semilla: cfg.semilla,
    relojSimulado: iso(reloj.ahora()),
    pausado: reloj.pausado,
    flujoActivo: motor.flujoActivo,
    factor: reloj.factor,
    parametros: motor.parametros,
    estadisticas: { ...motor.estadisticas, publicados, pendientesDePublicar: pendientes.length, accionesProgramadas: motor.pendientes },
    escenarios: listarEscenarios(),
  };
}

async function lagDeGrupos() {
  const grupos = [];
  for (const groupId of Object.values(GRUPOS)) {
    grupos.push(await calcularLag(admin, groupId, SUSCRIPCIONES[groupId]));
  }
  return { grupos, dlq: await contarDlq(admin), consultadoEn: new Date().toISOString() };
}

function responder(res, status, cuerpo) {
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(cuerpo));
}

async function leerCuerpo(req) {
  let texto = '';
  for await (const parte of req) texto += parte;
  if (!texto) return {};
  try {
    return JSON.parse(texto);
  } catch {
    return {};
  }
}

const servidor = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const ruta = url.pathname.replace(/\/+$/, '') || '/';
  try {
    if (req.method === 'GET' && (ruta === '/' || ruta === '/estado')) return responder(res, 200, estado());
    if (req.method === 'GET' && ruta === '/salud') return responder(res, 200, { ok: true });
    if (req.method === 'GET' && ruta === '/lag') return responder(res, 200, await lagDeGrupos());

    if (req.method === 'POST') {
      const cuerpo = await leerCuerpo(req);
      if (ruta === '/pausar') {
        reloj.pausar();
        return responder(res, 200, estado());
      }
      if (ruta === '/reanudar') {
        reloj.reanudar();
        return responder(res, 200, estado());
      }
      if (ruta === '/flujo/detener') {
        motor.detenerFlujo();
        return responder(res, 200, estado());
      }
      if (ruta === '/flujo/reanudar') {
        motor.reanudarFlujo();
        return responder(res, 200, estado());
      }
      if (ruta === '/parametros') {
        const permitidos = ['enviosPorMinuto', 'probDemoraHub', 'probDuplicado', 'probFueraDeOrden', 'probMalformado', 'probDemoraSincronizacion'];
        for (const k of permitidos) if (typeof cuerpo[k] === 'number') motor.parametros[k] = cuerpo[k];
        if (typeof cuerpo.factor === 'number' && cuerpo.factor > 0) {
          reloj.cambiarFactor(cuerpo.factor);
          motor.parametros.factor = cuerpo.factor;
        }
        return responder(res, 200, estado());
      }
      const m = ruta.match(/^\/escenarios\/([a-z-]+)$/);
      if (m) {
        const resultado = ejecutarEscenario(motor, m[1], reloj.ahora(), cuerpo);
        pendientes.push(...motor.tomarSalida());
        publicarPendientes();
        log.info('escenario disparado', resultado);
        return responder(res, 200, { ok: true, ...resultado, relojSimulado: iso(reloj.ahora()) });
      }
    }
    return responder(res, 404, { error: `ruta no encontrada: ${req.method} ${ruta}` });
  } catch (e) {
    log.error('error en la API', { ruta, error: e.message });
    return responder(res, 400, { error: e.message });
  }
});

async function iniciar() {
  await productor.connect();
  await admin.connect();
  log.info('simulador en marcha', {
    corrida: motor.corrida,
    semilla: cfg.semilla,
    reinicios,
    relojSimulado: iso(reloj.ahora()),
    factor: cfg.factor,
    precargaHoras: precargaMs / 3_600_000,
  });
  setInterval(tick, cfg.tickMs);
  setInterval(
    () => guardarEstadoPersistido(cfg.archivoEstado, { corrida, simMs: reloj.ahora(), reinicios }),
    2_000
  );
  servidor.listen(cfg.puerto, () => log.info('API del simulador escuchando', { puerto: cfg.puerto }));
}

async function cerrar(senal) {
  log.info('deteniendo simulador', { senal });
  guardarEstadoPersistido(cfg.archivoEstado, { corrida, simMs: reloj.ahora(), reinicios });
  servidor.close();
  try {
    await publicarPendientes();
    await productor.disconnect();
    await admin.disconnect();
  } finally {
    process.exit(0);
  }
}
process.once('SIGTERM', () => cerrar('SIGTERM'));
process.once('SIGINT', () => cerrar('SIGINT'));

iniciar().catch((e) => {
  log.fatal('no se pudo iniciar el simulador', { error: e.message });
  process.exit(1);
});
