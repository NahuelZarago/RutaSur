// Muestra el lag de cada grupo de consumidores y la cantidad de mensajes en la DLQ.
//   docker compose run --rm herramientas node scripts/lag.js
import { GRUPOS, SUSCRIPCIONES } from '../shared/eventos.js';
import { calcularLag, contarDlq, crearKafka } from '../shared/kafka.js';

const kafka = crearKafka('herramientas-lag');
const admin = kafka.admin();
await admin.connect();

const filas = [];
for (const groupId of Object.values(GRUPOS)) {
  const lag = await calcularLag(admin, groupId, SUSCRIPCIONES[groupId]);
  filas.push({
    grupo: groupId,
    estado: lag.iniciado ? lag.estado : 'no iniciado',
    miembros: lag.miembros,
    lag: lag.lagTotal,
    detalle: lag.topics.map((t) => `${t.topic}=${t.lag}`).join('  '),
  });
}
console.table(filas);
console.log(`Mensajes en ${'envios.estados.dlq'}: ${await contarDlq(admin)}`);
await admin.disconnect();
process.exit(0);
