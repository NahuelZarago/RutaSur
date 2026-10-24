// Formatos y etiquetas compartidos por servidor y cliente.
// Las fechas se muestran en hora de Argentina con un desplazamiento fijo (UTC-3), así el HTML del
// servidor y el del navegador coinciden sin depender de la zona horaria de cada máquina.
import catalogo from './catalogo.json';

const OFFSET = -3 * 3600_000;
const DIAS = ['dom', 'lun', 'mar', 'mié', 'jue', 'vie', 'sáb'];
const pad = (n) => String(n).padStart(2, '0');

function local(iso) {
  return new Date(Date.parse(iso) + OFFSET);
}

export function hora(iso) {
  if (!iso) return '—';
  const d = local(iso);
  return `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
}

export function diaHora(iso) {
  if (!iso) return '—';
  const d = local(iso);
  return `${DIAS[d.getUTCDay()]} ${pad(d.getUTCDate())}/${pad(d.getUTCMonth() + 1)} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
}

export function dia(fechaOIso) {
  if (!fechaOIso) return '—';
  const d = fechaOIso.length === 10 ? new Date(`${fechaOIso}T12:00:00Z`) : local(fechaOIso);
  return `${DIAS[d.getUTCDay()]} ${pad(d.getUTCDate())}/${pad(d.getUTCMonth() + 1)}`;
}

export const ESTADOS_RECORRIDO = ['CREADO', 'INGRESADO_HUB', 'CLASIFICADO', 'EN_REPARTO', 'ENTREGADO'];
export const ESTADOS_DESVIO = ['ENTREGA_FALLIDA', 'REPROGRAMADO', 'RETENIDO', 'DEVOLUCION'];

export const ETIQUETA_ESTADO = {
  CREADO: 'Creado',
  INGRESADO_HUB: 'En el hub',
  CLASIFICADO: 'Clasificado',
  EN_REPARTO: 'En reparto',
  ENTREGADO: 'Entregado',
  ENTREGA_FALLIDA: 'Fallido sin resolver',
  REPROGRAMADO: 'Reprogramado',
  RETENIDO: 'Retenido',
  DEVOLUCION: 'En devolución',
  SIN_ESTADO: 'Sin estado',
};

export const ETIQUETA_EVENTO = {
  CREADO: 'Creado por el comercio',
  INGRESADO_HUB: 'Ingresó al hub',
  CLASIFICADO: 'Clasificado',
  EN_REPARTO: 'Salió a reparto',
  ENTREGADO: 'Entregado',
  ENTREGA_FALLIDA: 'Entrega fallida',
};

export const ETIQUETA_MOTIVO = {
  AUSENTE: 'Ausente',
  DIRECCION_INCORRECTA: 'Dirección incorrecta',
  RECHAZADO: 'Rechazado',
  ZONA_INACCESIBLE: 'Zona inaccesible',
};

export const ETIQUETA_REGLA = {
  SIN_INGRESO: 'Sin ingreso al hub',
  SIN_SALIDA: 'Sin salida a reparto',
  FALLO_DIA_LIMITE: 'Fallo en el día límite',
  VENCIDO: 'Promesa vencida',
};

export const ETIQUETA_PRIORIDAD = { CRITICA: 'Crítica', ALTA: 'Alta', MEDIA: 'Media' };
export const ETIQUETA_FRANJA = { MANANA: 'Mañana', TARDE: 'Tarde' };

export const zonas = catalogo.zonas;
export const hubs = catalogo.hubs;
export const nombreZona = (id) => catalogo.zonas.find((z) => z.id === id)?.nombre ?? id ?? '—';
export const nombreHub = (id) => catalogo.hubs.find((h) => h.id === id)?.nombre ?? id ?? '—';

export function porcentaje(parte, total) {
  if (!total) return '—';
  return `${Math.round((100 * parte) / total)} %`;
}

export function ordenarAlertas(lista) {
  return [...lista].sort((a, b) => b.prioridadOrden - a.prioridadOrden || String(a.abiertaEn).localeCompare(String(b.abiertaEn)));
}

/** Texto pensado para el destinatario: sin hubs, repartidores ni alertas internas. */
export function textoPublico(envio) {
  const estado = envio.estadoVisible;
  const proximo = envio.resolucion?.proximoIntento;
  switch (estado) {
    case 'CREADO':
      return { titulo: 'Recibimos tu envío', detalle: `${envio.comercio ?? 'El comercio'} ya lo despachó.` };
    case 'INGRESADO_HUB':
      return { titulo: 'Está en nuestro centro de distribución', detalle: 'Lo estamos preparando para el reparto.' };
    case 'CLASIFICADO':
      return { titulo: 'Listo para salir', detalle: 'Ya tiene repartidor asignado para tu zona.' };
    case 'EN_REPARTO':
      return { titulo: 'Está en camino', detalle: 'Hoy lo vas a recibir.' };
    case 'ENTREGADO':
      return { titulo: 'Entregado', detalle: `Lo entregamos el ${diaHora(envio.entregadoEn)}.` };
    case 'REPROGRAMADO':
      return {
        titulo: 'No pudimos entregarlo',
        detalle: proximo ? `Volvemos a intentarlo el ${dia(proximo.fecha)} por la ${ETIQUETA_FRANJA[proximo.franja].toLowerCase()}.` : 'Vamos a reprogramar la visita.',
      };
    case 'RETENIDO':
      return {
        titulo: 'Necesitamos tu ayuda',
        detalle: envio.ultimoFallo?.motivo === 'DIRECCION_INCORRECTA' ? 'No pudimos ubicar la dirección. Revisá los datos de entrega.' : 'No pudimos acceder a la zona. Te avisaremos cuando reprogramemos.',
      };
    case 'DEVOLUCION':
      return { titulo: 'Vuelve al comercio', detalle: 'Comunicate con el comercio para coordinar una nueva entrega.' };
    case 'ENTREGA_FALLIDA':
      return { titulo: 'No pudimos entregarlo', detalle: 'Estamos definiendo el próximo intento.' };
    default:
      return { titulo: 'Envío registrado', detalle: 'Pronto vas a ver novedades.' };
  }
}

export function fechaEstimada(envio) {
  if (envio.estadoVisible === 'ENTREGADO') return null;
  if (envio.estadoVisible === 'REPROGRAMADO' && envio.resolucion?.proximoIntento) {
    const p = envio.resolucion.proximoIntento;
    return `${dia(p.fecha)}, ${ETIQUETA_FRANJA[p.franja].toLowerCase()}`;
  }
  if (['DEVOLUCION', 'RETENIDO'].includes(envio.estadoVisible)) return null;
  return envio.promesa?.venceEn ? `antes del ${diaHora(envio.promesa.venceEn)}` : null;
}
