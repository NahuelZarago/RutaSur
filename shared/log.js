// Logger estructurado: una línea JSON por registro, para poder filtrar por eventId, partición u offset
// (docker compose logs proyector | grep RS-XXXX-00012).
import { config } from './config.js';

const NIVELES = { debug: 10, info: 20, warn: 30, error: 40, fatal: 50 };
const minimo = NIVELES[config.logNivel] ?? NIVELES.info;

export function crearLogger(servicio) {
  const escribir = (nivel, msg, datos = {}) => {
    if (NIVELES[nivel] < minimo) return;
    const linea = JSON.stringify({ ts: new Date().toISOString(), nivel, servicio, msg, ...datos });
    if (NIVELES[nivel] >= NIVELES.warn) process.stderr.write(linea + '\n');
    else process.stdout.write(linea + '\n');
  };
  return {
    debug: (msg, datos) => escribir('debug', msg, datos),
    info: (msg, datos) => escribir('info', msg, datos),
    warn: (msg, datos) => escribir('warn', msg, datos),
    error: (msg, datos) => escribir('error', msg, datos),
    fatal: (msg, datos) => escribir('fatal', msg, datos),
  };
}
