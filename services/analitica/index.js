// Servicio de analítica (CU4): convierte eventos en incrementos atómicos sobre metricas_diarias.
// Los incrementos, el join de SLA y la marca de idempotencia se escriben en una sola transacción:
// un reintento nunca suma dos veces.
import { ejecutarConsumidor } from '../../shared/consumidor.js';
import { GRUPOS, SUSCRIPCIONES, TOPICS } from '../../shared/eventos.js';
import { crearKafka } from '../../shared/kafka.js';
import { crearLogger } from '../../shared/log.js';
import { asegurarIndices, conectarMongo } from '../../shared/mongo.js';
import { aplicarJoinSla, idMetrica, incrementosPorEstado, participaEnJoinSla } from '../../shared/dominio/analitica.js';

const SERVICIO = 'analitica';
const log = crearLogger(SERVICIO);

async function main() {
  const { client, db } = await conectarMongo(log);
  await asegurarIndices(db, log);
  const metricas = db.collection('metricas_diarias');
  const joins = db.collection('analitica_promesas');

  await ejecutarConsumidor({
    servicio: SERVICIO,
    groupId: GRUPOS.ANALITICA,
    topics: SUSCRIPCIONES[GRUPOS.ANALITICA],
    kafka: crearKafka(SERVICIO),
    client,
    db,
    transaccional: true,
    manejar: async (evento, { session, meta }) => {
      const incrementos = meta.topic === TOPICS.ESTADOS ? incrementosPorEstado(evento) : [];

      if (participaEnJoinSla(meta.topic, evento)) {
        const actual = await joins.findOne({ _id: evento.shipmentId }, { session });
        const r = aplicarJoinSla(actual, meta.topic, evento);
        await joins.replaceOne({ _id: evento.shipmentId }, r.join, { upsert: true, session });
        incrementos.push(...r.incrementos);
      }

      for (const { clave, inc } of incrementos) {
        await metricas.updateOne(
          { _id: idMetrica(clave) },
          { $inc: inc, $setOnInsert: { fecha: clave.fecha, zonaId: clave.zonaId, hubId: clave.hubId, franja: clave.franja } },
          { upsert: true, session }
        );
      }
      return incrementos.length ? `agregado(${incrementos.length})` : 'ignorado';
    },
  });
}

main().catch((e) => {
  log.fatal('el servicio de analítica terminó con error', { error: e.message, stack: e.stack });
  process.exit(1);
});
