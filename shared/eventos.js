// Catálogo de eventos: topics, tipos, grupos de consumidores y suscripciones.
import { DIA } from './tiempo.js';

export const VERSION_ESQUEMA = 1;
export const HEADER_RELOJ = 'x-reloj-simulado';

export const TOPICS = {
  ESTADOS: 'envios.estados',
  PROMESAS: 'envios.promesas',
  REPROGRAMACIONES: 'envios.reprogramaciones',
  ALERTAS: 'alertas.sla',
  NOTIFICACIONES: 'notificaciones.enviadas',
  DLQ: 'envios.estados.dlq',
};

const retencion = (dias) => ({ name: 'retention.ms', value: String(dias * DIA) });

/** Definición de los seis topics. La clave de partición es siempre el shipmentId. */
export const DEFINICION_TOPICS = [
  { topic: TOPICS.ESTADOS, numPartitions: 3, replicationFactor: 1, configEntries: [retencion(30)] },
  {
    topic: TOPICS.PROMESAS,
    numPartitions: 3,
    replicationFactor: 1,
    configEntries: [{ name: 'cleanup.policy', value: 'compact' }],
  },
  { topic: TOPICS.REPROGRAMACIONES, numPartitions: 3, replicationFactor: 1, configEntries: [retencion(30)] },
  { topic: TOPICS.ALERTAS, numPartitions: 3, replicationFactor: 1, configEntries: [retencion(7)] },
  { topic: TOPICS.NOTIFICACIONES, numPartitions: 3, replicationFactor: 1, configEntries: [retencion(7)] },
  { topic: TOPICS.DLQ, numPartitions: 3, replicationFactor: 1, configEntries: [retencion(30)] },
];

export const TIPOS_ESTADO = [
  'CREADO',
  'INGRESADO_HUB',
  'CLASIFICADO',
  'EN_REPARTO',
  'ENTREGADO',
  'ENTREGA_FALLIDA',
];

export const TIPOS_CON_INTENTO = ['EN_REPARTO', 'ENTREGADO', 'ENTREGA_FALLIDA'];

export const MOTIVOS_FALLO = ['AUSENTE', 'DIRECCION_INCORRECTA', 'RECHAZADO', 'ZONA_INACCESIBLE'];

export const TIPOS_REPROGRAMACION = ['REPROGRAMADO', 'RETENIDO', 'DEVOLUCION'];

export const REGLAS_SLA = {
  SIN_INGRESO: { prioridad: 'MEDIA', nombre: 'Sin ingreso' },
  SIN_SALIDA: { prioridad: 'ALTA', nombre: 'Sin salida' },
  FALLO_DIA_LIMITE: { prioridad: 'ALTA', nombre: 'Fallo en el día límite' },
  VENCIDO: { prioridad: 'CRITICA', nombre: 'Vencido' },
};

export const ORDEN_PRIORIDAD = { MEDIA: 1, ALTA: 2, CRITICA: 3 };

export const GRUPOS = {
  PROYECTOR: 'proyector',
  NOTIFICACIONES: 'notificaciones',
  REPROGRAMACION: 'reprogramacion',
  MONITOR_SLA: 'monitor-sla',
  ANALITICA: 'analitica',
  TIEMPOS_HUB: 'analitica-tiempos-hub',
};

/** Qué topics lee cada grupo de consumidores. */
export const SUSCRIPCIONES = {
  [GRUPOS.PROYECTOR]: [
    TOPICS.ESTADOS,
    TOPICS.PROMESAS,
    TOPICS.REPROGRAMACIONES,
    TOPICS.ALERTAS,
    TOPICS.NOTIFICACIONES,
  ],
  [GRUPOS.NOTIFICACIONES]: [TOPICS.ESTADOS, TOPICS.ALERTAS],
  [GRUPOS.REPROGRAMACION]: [TOPICS.ESTADOS],
  [GRUPOS.MONITOR_SLA]: [TOPICS.ESTADOS, TOPICS.PROMESAS],
  [GRUPOS.ANALITICA]: [TOPICS.ESTADOS, TOPICS.PROMESAS],
  [GRUPOS.TIEMPOS_HUB]: [TOPICS.ESTADOS],
};
