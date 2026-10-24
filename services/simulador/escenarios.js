// Escenarios que se disparan a pedido durante la demo. Cada escenario crea uno o más envíos con un
// guion de tiempos (en tiempo simulado, relativo al paso anterior) y resultados predefinidos.
// Los envíos de escenario no tienen anomalías aleatorias: el resultado es siempre el mismo.
import { finDiaLocal, HORA, MIN } from '../../shared/tiempo.js';

const CORTO = { ingreso: 20 * MIN, clasificado: 20 * MIN, salida: 20 * MIN, resultado: 30 * MIN };

export const ESCENARIOS = {
  feliz: {
    titulo: 'Envío feliz',
    descripcion: 'Recorre todos los estados sin incidentes (≈ 45 s reales).',
    guion: () => ({ ...CORTO, venceEn: (t) => t + 8 * HORA, resultados: ['OK'] }),
  },
  ausente: {
    titulo: 'Ausente',
    descripcion: 'Falla por ausencia y se reprograma al día hábil siguiente en la franja opuesta.',
    guion: () => ({ ...CORTO, venceEn: (t) => t + 8 * HORA, resultados: ['AUSENTE', 'OK'] }),
  },
  demorado: {
    titulo: 'Demorado en hub',
    descripcion:
      'Ingresa al hub y queda detenido. La alerta "Sin salida" (alta) aparece unos 3 min reales después, antes del vencimiento; luego sale y se entrega a tiempo.',
    guion: () => ({
      venceEn: (t) => t + 10.5 * HORA,
      ingreso: 15 * MIN,
      clasificado: 15 * MIN,
      salida: 7 * HORA,
      resultado: 1 * HORA,
      resultados: ['OK'],
    }),
  },
  'tercer-fallo': {
    titulo: 'Tercer fallo',
    descripcion: 'Tres ausencias seguidas (reintentos acelerados): pasa a devolución.',
    guion: () => ({
      ...CORTO,
      venceEn: (t) => t + 3 * 24 * HORA,
      resultados: ['AUSENTE', 'AUSENTE', 'AUSENTE'],
      reintento: 45 * MIN,
    }),
  },
  rafaga: {
    titulo: 'Ráfaga',
    descripcion: '500 envíos en 5 segundos para ver acumulación de lag y recuperación.',
    rafaga: 500,
  },
  'sin-ingreso': {
    titulo: 'Sin ingreso',
    descripcion: 'No llega al hub a tiempo: alerta media; ingresa tarde y se entrega.',
    guion: () => ({
      venceEn: (t) => t + 12 * HORA,
      ingreso: 5 * HORA,
      clasificado: 30 * MIN,
      salida: 30 * MIN,
      resultado: 30 * MIN,
      resultados: ['OK'],
    }),
  },
  vencido: {
    titulo: 'Vencido',
    descripcion: 'Promesa ajustada y nunca sale a reparto: sin ingreso, sin salida y vencido (crítica) en ≈ 1 min.',
    guion: () => ({ venceEn: (t) => t + 2 * HORA, ingreso: 15 * MIN, clasificado: 15 * MIN, salida: null }),
  },
  'fallo-dia-limite': {
    titulo: 'Fallo en el día límite',
    descripcion: 'La promesa vence hoy y el intento falla por ausencia.',
    guion: () => ({
      ingreso: 15 * MIN,
      clasificado: 15 * MIN,
      salida: 15 * MIN,
      resultado: 30 * MIN,
      venceEn: (t) => finDiaLocal(t + 75 * MIN) - MIN + 1,
      resultados: ['AUSENTE', 'OK'],
    }),
  },
  'direccion-incorrecta': {
    titulo: 'Dirección incorrecta',
    descripcion: 'Falla por dirección: queda retenido hasta una corrección.',
    guion: () => ({ ...CORTO, venceEn: (t) => t + 8 * HORA, resultados: ['DIRECCION_INCORRECTA'] }),
  },
  rechazado: {
    titulo: 'Rechazado',
    descripcion: 'El destinatario rechaza el paquete: devolución al comercio.',
    guion: () => ({ ...CORTO, venceEn: (t) => t + 8 * HORA, resultados: ['RECHAZADO'] }),
  },
  'zona-inaccesible': {
    titulo: 'Zona inaccesible',
    descripcion: 'No se puede acceder a la zona: retenido en el hub.',
    guion: () => ({ ...CORTO, venceEn: (t) => t + 8 * HORA, resultados: ['ZONA_INACCESIBLE'] }),
  },
  duplicado: {
    titulo: 'Evento duplicado',
    descripcion: 'INGRESADO_HUB se publica dos veces con el mismo eventId: el historial no se repite.',
    guion: () => ({ ...CORTO, venceEn: (t) => t + 8 * HORA, resultados: ['OK'], anomalias: { duplicar: 'INGRESADO_HUB' } }),
  },
  'fuera-de-orden': {
    titulo: 'Evento fuera de orden',
    descripcion: 'INGRESADO_HUB llega después de CLASIFICADO: el estado no retrocede.',
    guion: () => ({ ...CORTO, venceEn: (t) => t + 8 * HORA, resultados: ['OK'], anomalias: { fueraDeOrden: 'INGRESADO_HUB' } }),
  },
  malformado: {
    titulo: 'Evento malformado',
    descripcion: 'Publica un mensaje que no cumple el esquema: termina en la DLQ y el flujo sigue.',
    malformado: true,
  },
};

/**
 * Ejecuta un escenario sobre el motor en el instante simulado ahoraMs.
 * @returns {{escenario: string, shipmentIds: string[], detalle?: object}}
 */
export function ejecutarEscenario(motor, nombre, ahoraMs, opciones = {}) {
  const def = ESCENARIOS[nombre];
  if (!def) throw new Error(`Escenario desconocido: ${nombre}`);

  if (def.malformado) {
    const id = `RS-${motor.corrida}-MAL${String(motor.contadores.malformados + 1).padStart(3, '0')}`;
    const base = {
      eventId: `${id}-01`,
      shipmentId: id,
      tipo: 'EN_REPARTO',
      ocurridoEn: new Date(ahoraMs).toISOString(),
      secuencia: 4,
      hubId: 'HUB-CENTRO',
      zonaId: 'Z-C1',
      repartidorId: 'R-040',
      intento: 1,
      franja: 'MANANA',
      version: 1,
    };
    const detalle = motor.publicarMalformado(id, base, ahoraMs, opciones.clase ?? null);
    return { escenario: nombre, shipmentIds: [id], detalle };
  }

  if (def.rafaga) {
    const ventanaSim = 5_000 * motor.parametros.factor; // 5 segundos reales
    for (let i = 0; i < def.rafaga; i++) {
      motor.programar(ahoraMs + Math.floor((i * ventanaSim) / def.rafaga), { tipo: 'crearRafaga' });
    }
    return { escenario: nombre, shipmentIds: [], detalle: { envios: def.rafaga } };
  }

  const id = motor.crearEnvio(ahoraMs, { origen: 'escenario', guion: def.guion(), zonaId: opciones.zonaId ?? null });
  return { escenario: nombre, shipmentIds: [id] };
}

export function listarEscenarios() {
  return Object.entries(ESCENARIOS).map(([id, e]) => ({ id, titulo: e.titulo, descripcion: e.descripcion }));
}
