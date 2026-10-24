// Servicio de notificaciones (CU2/CU3): registra avisos al destinatario ante entregas fallidas y
// alertas de SLA. No envía correos ni SMS reales: publica la notificación en
// notificaciones.enviadas (el proyector la guarda en MongoDB) y la deja en el log.
import { ejecutarConsumidor } from '../../shared/consumidor.js';
import { GRUPOS, SUSCRIPCIONES, TOPICS } from '../../shared/eventos.js';
import { crearKafka, crearProductor } from '../../shared/kafka.js';
import { crearLogger } from '../../shared/log.js';
import { asegurarIndices, conectarMongo } from '../../shared/mongo.js';
import { decidirNotificacion } from '../../shared/dominio/notificaciones.js';

const SERVICIO = 'notificaciones';
const log = crearLogger(SERVICIO);

async function main() {
  const { client, db } = await conectarMongo(log);
  await asegurarIndices(db, log);
  const kafka = crearKafka(SERVICIO);
  const productor = crearProductor(kafka);
  await productor.connect();

  await ejecutarConsumidor({
    servicio: SERVICIO,
    groupId: GRUPOS.NOTIFICACIONES,
    topics: SUSCRIPCIONES[GRUPOS.NOTIFICACIONES],
    kafka,
    client,
    db,
    manejar: async (evento, { meta }) => {
      const notificacion = decidirNotificacion(meta.topic, evento);
      if (!notificacion) return 'ignorado';
      await productor.send({
        topic: TOPICS.NOTIFICACIONES,
        acks: -1,
        messages: [{ key: notificacion.shipmentId, value: JSON.stringify(notificacion) }],
      });
      log.info('notificación simulada', {
        eventId: notificacion.eventId,
        shipmentId: notificacion.shipmentId,
        canal: notificacion.canal,
        plantilla: notificacion.plantilla,
        mensaje: notificacion.mensaje,
      });
      return 'notificado';
    },
  });
}

main().catch((e) => {
  log.fatal('el servicio de notificaciones terminó con error', { error: e.message, stack: e.stack });
  process.exit(1);
});
