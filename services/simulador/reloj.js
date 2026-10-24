// Reloj simulado: comprime el tiempo según el factor de aceleración (120 → 1 hora simulada = 30 s).
// Se puede pausar; al pausar, todo el sistema se congela porque los demás servicios solo conocen el
// tiempo a través de los eventos.
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';

export class RelojSimulado {
  constructor({ inicioMs, factor }) {
    this.baseSim = inicioMs;
    this.anclaReal = Date.now();
    this.factor = factor;
    this.pausado = false;
  }

  ahora() {
    return this.pausado ? this.baseSim : this.baseSim + (Date.now() - this.anclaReal) * this.factor;
  }

  pausar() {
    if (this.pausado) return;
    this.baseSim = this.ahora();
    this.pausado = true;
  }

  reanudar() {
    if (!this.pausado) return;
    this.anclaReal = Date.now();
    this.pausado = false;
  }

  cambiarFactor(factor) {
    this.baseSim = this.ahora();
    this.anclaReal = Date.now();
    this.factor = factor;
  }
}

/** Persistencia mínima para que un reinicio del simulador no haga retroceder el reloj. */
export function leerEstadoPersistido(archivo) {
  try {
    return JSON.parse(readFileSync(archivo, 'utf8'));
  } catch {
    return null;
  }
}

export function guardarEstadoPersistido(archivo, estado) {
  try {
    mkdirSync(dirname(archivo), { recursive: true });
    writeFileSync(archivo, JSON.stringify(estado));
  } catch {
    // Sin volumen montado: el reinicio comenzará una corrida nueva.
  }
}
