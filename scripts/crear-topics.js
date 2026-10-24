// Crea los seis topics con su configuración (particiones, retención, compactación).
// Lo ejecuta el contenedor kafka-init antes de que arranquen los servicios.
import { asegurarTopics, crearKafka } from '../shared/kafka.js';
import { DEFINICION_TOPICS } from '../shared/eventos.js';
import { crearLogger } from '../shared/log.js';
import { reintentar } from '../shared/mongo.js';

const log = crearLogger('kafka-init');

const resultado = await reintentar(() => asegurarTopics(crearKafka('kafka-init')), {
  intentos: 30,
  pausaMs: 2000,
  log,
  descripcion: 'creación de topics',
});

for (const t of DEFINICION_TOPICS) {
  log.info(resultado.creados.includes(t.topic) ? 'topic creado' : 'topic existente', {
    topic: t.topic,
    particiones: t.numPartitions,
    config: Object.fromEntries(t.configEntries.map((c) => [c.name, c.value])),
  });
}
process.exit(0);
