// Servicio de reprogramación (CU2): decide el próximo paso tras una ENTREGA_FALLIDA según el motivo
// y el número de intento, y publica la decisión en envios.reprogramaciones.
import { ejecutarConsumidor } from '../../shared/consumidor.js';
import { GRUPOS, SUSCRIPCIONES, TOPICS } from '../../shared/eventos.js';
import { crearKafka, crearProductor } from '../../shared/kafka.js';
import { crearLogger } from '../../shared/log.js';
import { asegurarIndices, conectarMongo } from '../../shared/mongo.js';
import { eventoReprogramacion } from '../../shared/dominio/reprogramacion.js';

const SERVICIO = 'reprogramacion';
const log = crearLogger(SERVICIO);

async function main() {
  const { client, db } = await conectarMongo(log);
  await asegurarIndices(db, log);
  const kafka = crearKafka(SERVICIO);
  const productor = crearProductor(kafka);
  await productor.connect();

  await ejecutarConsumidor({
    servicio: SERVICIO,
    groupId: GRUPOS.REPROGRAMACION,
    topics: SUSCRIPCIONES[GRUPOS.REPROGRAMACION],
    kafka,
    client,
    db,
    manejar: async (evento) => {
      if (evento.tipo !== 'ENTREGA_FALLIDA') return 'ignorado';
      const decision = eventoReprogramacion(evento);
      await productor.send({
        topic: TOPICS.REPROGRAMACIONES,
        acks: -1,
        messages: [{ key: decision.shipmentId, value: JSON.stringify(decision) }],
      });
      log.info('decisión de reprogramación', {
        eventId: decision.eventId,
        shipmentId: decision.shipmentId,
        tipo: decision.tipo,
        regla: decision.regla,
        proximoIntento: decision.proximoIntento ?? null,
      });
      return decision.tipo.toLowerCase();
    },
  });
}

main().catch((e) => {
  log.fatal('el servicio de reprogramación terminó con error', { error: e.message, stack: e.stack });
  process.exit(1);
});
