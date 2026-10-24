// Proyector (CU1): mantiene las vistas envios, alertas y notificaciones en MongoDB.
// Es el único que escribe esas colecciones; Next.js solo las lee.
//
// Consume envios.estados, envios.promesas, envios.reprogramaciones, alertas.sla y
// notificaciones.enviadas. Cada evento se aplica en una transacción junto con su marca en
// eventos_procesados (idempotencia) y el offset se confirma después.
import { ejecutarConsumidor } from '../../shared/consumidor.js';
import { GRUPOS, SUSCRIPCIONES, TOPICS } from '../../shared/eventos.js';
import { crearKafka } from '../../shared/kafka.js';
import { crearLogger } from '../../shared/log.js';
import { asegurarIndices, conectarMongo } from '../../shared/mongo.js';
import { documentoNotificacion, idAlerta, reducirAlerta, reducirEnvio } from '../../shared/dominio/proyeccion.js';

const SERVICIO = 'proyector';
const log = crearLogger(SERVICIO);

async function main() {
  const { client, db } = await conectarMongo(log);
  await asegurarIndices(db, log);
  const envios = db.collection('envios');
  const alertas = db.collection('alertas');
  const notificaciones = db.collection('notificaciones');

  await ejecutarConsumidor({
    servicio: SERVICIO,
    groupId: GRUPOS.PROYECTOR,
    topics: SUSCRIPCIONES[GRUPOS.PROYECTOR],
    kafka: crearKafka(SERVICIO),
    client,
    db,
    transaccional: true,
    manejar: async (evento, { session, meta }) => {
      const actual = await envios.findOne({ _id: evento.shipmentId }, { session });
      const { envio, resultado } = reducirEnvio(actual, meta.topic, evento, meta);
      await envios.replaceOne({ _id: evento.shipmentId }, envio, { upsert: true, session });

      if (meta.topic === TOPICS.ALERTAS) {
        const id = idAlerta(evento);
        const previa = await alertas.findOne({ _id: id }, { session });
        await alertas.replaceOne({ _id: id }, reducirAlerta(previa, evento), { upsert: true, session });
      }
      if (meta.topic === TOPICS.NOTIFICACIONES) {
        await notificaciones.replaceOne({ _id: evento.eventId }, documentoNotificacion(evento, meta), { upsert: true, session });
      }
      return resultado;
    },
  });
}

main().catch((e) => {
  log.fatal('el proyector terminó con error', { error: e.message, stack: e.stack });
  process.exit(1);
});
