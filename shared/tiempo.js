// Utilidades de tiempo en hora local de Argentina (UTC-3, sin horario de verano).
// Todo el sistema razona en "tiempo de evento" (ocurridoEn, reloj simulado), nunca en el reloj
// de la máquina. Las fechas viajan como strings ISO-8601 en UTC.

export const MIN = 60_000;
export const HORA = 60 * MIN;
export const DIA = 24 * HORA;
export const OFFSET_LOCAL_MS = -3 * HORA;

export const FRANJAS = {
  MANANA: { id: 'MANANA', nombre: 'Mañana', desde: 8, hasta: 13 },
  TARDE: { id: 'TARDE', nombre: 'Tarde', desde: 13, hasta: 19 },
};

const aMs = (t) => (typeof t === 'number' ? t : Date.parse(t));
const mod = (a, n) => ((a % n) + n) % n;
const pad = (n) => String(n).padStart(2, '0');

function partes(t) {
  const d = new Date(aMs(t) + OFFSET_LOCAL_MS);
  return {
    anio: d.getUTCFullYear(),
    mes: d.getUTCMonth() + 1,
    dia: d.getUTCDate(),
    hora: d.getUTCHours(),
    minuto: d.getUTCMinutes(),
    diaSemana: d.getUTCDay(),
  };
}

export const iso = (ms) => new Date(ms).toISOString();

/** 'YYYY-MM-DD' en hora local. */
export function fechaLocal(t) {
  const p = partes(t);
  return `${p.anio}-${pad(p.mes)}-${pad(p.dia)}`;
}

/** Hora local decimal (14.5 = 14:30). */
export function horaLocal(t) {
  const p = partes(t);
  return p.hora + p.minuto / 60;
}

export function inicioDiaLocal(t) {
  const local = aMs(t) + OFFSET_LOCAL_MS;
  return local - mod(local, DIA) - OFFSET_LOCAL_MS;
}

export function finDiaLocal(t) {
  return inicioDiaLocal(t) + DIA - 1;
}

/** Mismo día local que t, a la hora h:m. */
export function aHoraLocal(t, h, m = 0) {
  return inicioDiaLocal(t) + h * HORA + m * MIN;
}

export function esDiaHabil(t) {
  const d = partes(t).diaSemana;
  return d >= 1 && d <= 5;
}

/** Medianoche local del día hábil siguiente a t (no considera feriados). */
export function siguienteDiaHabil(t) {
  let d = inicioDiaLocal(t) + DIA;
  while (!esDiaHabil(d)) d += DIA;
  return d;
}

export function mismoDiaLocal(a, b) {
  return fechaLocal(a) === fechaLocal(b);
}

export function franjaDe(t) {
  return horaLocal(t) < FRANJAS.TARDE.desde ? 'MANANA' : 'TARDE';
}

export function franjaOpuesta(franja) {
  return franja === 'MANANA' ? 'TARDE' : 'MANANA';
}
