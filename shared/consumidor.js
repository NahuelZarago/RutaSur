// Ejecutor genérico de consumidores. Concentra las garantías comunes a todos los servicios:
//
//  1. Validación de esquema: un mensaje inválido va a envios.estados.dlq con el motivo y el
//     consumo continúa.
//  2. Idempotencia: cada eventId aplicado se registra en eventos_procesados (por consumidor).
//     Un duplicado se reconoce y no vuelve a producir efectos.
//  3. Confirmación manual de offsets: el offset se confirma recién después de escribir en MongoDB
//     (o de publicar en Kafka). Si el proceso cae a mitad de camino, el evento se reintenta.
//  4. Observabilidad: cada mensaje se registra con eventId, topic, partición y offset.
//
// Modo transaccional: los efectos en MongoDB y la marca de "procesado" se escriben en la misma
// transacción (Mongo corre como replica set), así un reintento nunca duplica un incremento.
// Modo simple: para servicios cuyo efecto es publicar en Kafka. Publican eventos con eventId
// determinista, de modo que un reintento genera el mismo eventId y aguas abajo se descarta.
import { config } from './config.js';
import { decodificarMensaje } from './esquemas.js';
import { TOPICS } from './eventos.js';
import { crearProductor, headersATexto } from './kafka.js';
import { crearLogger } from './log.js';

const MAX_INTENTOS_POR_MENSAJE = 3;

function esErrorDeInfraestructura(e) {
  const nombre = e?.name ?? '';
  return (
    /MongoNetwork|MongoServerSelection|MongoNotConnected|MongoTopologyClosed|PoolClosed|KafkaJS/.test(nombre) ||
    e?.hasErrorLabel?.('TransientTransactionError') ||
    e?.hasErrorLabel?.('RetryableWriteError')
  );
}

/**
 * @param {object} opciones
 * @param {string} opciones.servicio      nombre del consumidor (se usa en eventos_procesados y logs)
 * @param {string} opciones.groupId       grupo de consumidores de Kafka
 * @param {string[]} opciones.topics      topics a los que se suscribe
 * @param {import('kafkajs').Kafka} opciones.kafka
 * @param {import('mongodb').MongoClient} opciones.client
 * @param {import('mongodb').Db} opciones.db
 * @param {(evento: object, ctx: {session?: any, meta: object}) => Promise<string|void>} opciones.manejar
 * @param {boolean} [opciones.transaccional]
 */
export async function ejecutarConsumidor({
  servicio,
  groupId,
  topics,
  kafka,
  client,
  db,
  manejar,
  transaccional = false,
  concurrencia = config.consumoConcurrente,
}) {
  const log = crearLogger(servicio);
  const procesados = db.collection('eventos_procesados');
  const consumer = kafka.consumer({
    groupId,
    sessionTimeout: 30_000,
    heartbeatInterval: 3_000,
    allowAutoTopicCreation: false,
  });
  const productorDlq = crearProductor(kafka);
  const intentosFallidos = new Map();

  await productorDlq.connect();
  await consumer.connect();
  // fromBeginning solo aplica cuando el grupo no tiene offsets confirmados: un grupo nuevo lee el
  // histórico completo; un grupo existente retoma desde su último offset.
  await consumer.subscribe({ topics, fromBeginning: true });

  consumer.on(consumer.events.GROUP_JOIN, (e) =>
    log.info('unido al grupo', { groupId, asignacion: e.payload.memberAssignment })
  );
  consumer.on(consumer.events.CRASH, (e) => {
    log.error('consumidor caído', { error: e.payload.error?.message, reinicia: e.payload.restart });
    if (!e.payload.restart) process.exit(1);
  });

  async function enviarADlq({ topic, partition, message, motivo }) {
    const clave = message.key?.toString() ?? 'desconocido';
    const registro = {
      eventId: `dlq:${servicio}:${topic}:${partition}:${message.offset}`,
      consumidor: servicio,
      topicOrigen: topic,
      particion: partition,
      offset: message.offset,
      claveOriginal: clave,
      motivo,
      valorOriginal: (message.value?.toString('utf8') ?? '').slice(0, 10_000),
      detectadoEn: new Date().toISOString(),
    };
    await productorDlq.send({ topic: TOPICS.DLQ, messages: [{ key: clave, value: JSON.stringify(registro) }] });
    log.warn('mensaje enviado a la DLQ', { topic, particion: partition, offset: message.offset, motivo });
  }

  async function procesarTransaccional(evento, meta) {
    const session = client.startSession();
    try {
      let resultado;
      await session.withTransaction(async () => {
        resultado = 'aplicado';
        const ya = await procesados.findOne(
          { consumidor: servicio, eventId: evento.eventId },
          { session, projection: { _id: 1 } }
        );
        if (ya) {
          resultado = 'duplicado';
          return;
        }
        resultado = (await manejar(evento, { session, meta })) ?? 'aplicado';
        await procesados.insertOne(
          { consumidor: servicio, eventId: evento.eventId, topic: meta.topic, particion: meta.partition, offset: meta.offset, procesadoEn: new Date() },
          { session }
        );
      });
      return resultado;
    } finally {
      await session.endSession();
    }
  }

  async function procesarSimple(evento, meta) {
    const ya = await procesados.findOne({ consumidor: servicio, eventId: evento.eventId }, { projection: { _id: 1 } });
    if (ya) return 'duplicado';
    const resultado = (await manejar(evento, { meta })) ?? 'aplicado';
    try {
      await procesados.insertOne({
        consumidor: servicio,
        eventId: evento.eventId,
        topic: meta.topic,
        particion: meta.partition,
        offset: meta.offset,
        procesadoEn: new Date(),
      });
    } catch (e) {
      if (e.code !== 11000) throw e;
    }
    return resultado;
  }

  await consumer.run({
    autoCommit: false,
    partitionsConsumedConcurrently: concurrencia,
    eachMessage: async ({ topic, partition, message }) => {
      const meta = {
        topic,
        partition,
        offset: message.offset,
        kafkaTs: Number(message.timestamp),
        headers: headersATexto(message.headers),
      };
      const claveIntento = `${topic}:${partition}:${message.offset}`;
      const decodificado = decodificarMensaje(topic, message.key, message.value);

      if (!decodificado.ok) {
        await enviarADlq({ topic, partition, message, motivo: decodificado.errores.join('; ') });
      } else {
        const evento = decodificado.evento;
        try {
          const resultado = transaccional
            ? await procesarTransaccional(evento, meta)
            : await procesarSimple(evento, meta);
          intentosFallidos.delete(claveIntento);
          log.info('evento procesado', {
            eventId: evento.eventId,
            shipmentId: evento.shipmentId,
            tipo: evento.tipo,
            topic,
            particion: partition,
            offset: message.offset,
            resultado,
          });
        } catch (e) {
          if (esErrorDeInfraestructura(e)) throw e; // se reintenta indefinidamente sin confirmar
          const intentos = (intentosFallidos.get(claveIntento) ?? 0) + 1;
          intentosFallidos.set(claveIntento, intentos);
          log.error('error procesando evento', { eventId: evento.eventId, topic, particion: partition, offset: message.offset, intentos, error: e.message });
          if (intentos < MAX_INTENTOS_POR_MENSAJE) throw e;
          intentosFallidos.delete(claveIntento);
          await enviarADlq({ topic, partition, message, motivo: `error de procesamiento tras ${intentos} intentos: ${e.message}` });
        }
      }
      // Confirmación manual: offset del próximo mensaje a leer.
      await consumer.commitOffsets([{ topic, partition, offset: (BigInt(message.offset) + 1n).toString() }]);
    },
  });

  log.info('consumidor en marcha', { groupId, topics, transaccional });

  const cerrar = async (senal) => {
    log.info('deteniendo consumidor', { senal });
    try {
      await consumer.disconnect();
      await productorDlq.disconnect();
      await client.close();
    } finally {
      process.exit(0);
    }
  };
  process.once('SIGTERM', () => cerrar('SIGTERM'));
  process.once('SIGINT', () => cerrar('SIGINT'));

  return { consumer, log };
}
