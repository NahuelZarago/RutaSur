// Cuarto indicador (demostración de reprocesamiento): tiempo medio entre INGRESADO_HUB y EN_REPARTO
// por hub. Es un grupo de consumidores NUEVO (analitica-tiempos-hub) que se activa con el sistema ya
// en marcha: como no tiene offsets confirmados, lee envios.estados desde el inicio y completa el
// histórico sin tocar a los demás servicios (cada grupo tiene sus propios offsets).
//
//   docker compose --profile replay up -d analitica-tiempos
import { ejecutarConsumidor } from '../../shared/consumidor.js';
import { GRUPOS, SUSCRIPCIONES } from '../../shared/eventos.js';
import { crearKafka } from '../../shared/kafka.js';
import { crearLogger } from '../../shared/log.js';
import { asegurarIndices, conectarMongo } from '../../shared/mongo.js';
import { aplicarTiemposHub, idMetricaTiempo } from '../../shared/dominio/analitica.js';

const SERVICIO = 'analitica-tiempos-hub';
const log = crearLogger(SERVICIO);

async function main() {
  const { client, db } = await conectarMongo(log);
  await asegurarIndices(db, log);
  const pendientes = db.collection('tiempos_hub_pendientes');
  const metricas = db.collection('metricas_tiempo_hub');

  await ejecutarConsumidor({
    servicio: SERVICIO,
    groupId: GRUPOS.TIEMPOS_HUB,
    topics: SUSCRIPCIONES[GRUPOS.TIEMPOS_HUB],
    kafka: crearKafka(SERVICIO),
    client,
    db,
    transaccional: true,
    manejar: async (evento, { session }) => {
      if (evento.tipo !== 'INGRESADO_HUB' && evento.tipo !== 'EN_REPARTO') return 'ignorado';
      const actual = await pendientes.findOne({ _id: evento.shipmentId }, { session });
      const { pendiente, incremento } = aplicarTiemposHub(actual, evento);
      await pendientes.replaceOne({ _id: evento.shipmentId }, pendiente, { upsert: true, session });
      if (!incremento) return 'pendiente';
      await metricas.updateOne(
        { _id: idMetricaTiempo(incremento.clave) },
        { $inc: incremento.inc, $setOnInsert: incremento.clave },
        { upsert: true, session }
      );
      return 'agregado';
    },
  });
}

main().catch((e) => {
  log.fatal('el servicio de tiempos de hub terminó con error', { error: e.message, stack: e.stack });
  process.exit(1);
});
