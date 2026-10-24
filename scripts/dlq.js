// Revisión manual de la DLQ: lista los mensajes rechazados con su motivo.
//   docker compose run --rm herramientas node scripts/dlq.js [--ultimos 20]
import { TOPICS } from '../shared/eventos.js';
import { crearKafka } from '../shared/kafka.js';
import { leerTopicCompleto } from './comun.js';

const i = process.argv.indexOf('--ultimos');
const ultimos = i > 0 ? Number(process.argv[i + 1]) : 20;

const mensajes = await leerTopicCompleto(crearKafka('herramientas-dlq'), [TOPICS.DLQ]);
mensajes.sort((a, b) => a.timestamp - b.timestamp);

console.log(`DLQ: ${mensajes.length} mensajes (se muestran los últimos ${Math.min(ultimos, mensajes.length)})\n`);
for (const m of mensajes.slice(-ultimos)) {
  let r;
  try {
    r = JSON.parse(m.value);
  } catch {
    r = { motivo: '(registro ilegible)', valorOriginal: m.value };
  }
  console.log(`· ${new Date(m.timestamp).toISOString()}  clave=${m.key}  detectado por: ${r.consumidor}`);
  console.log(`  origen: ${r.topicOrigen} p${r.particion} offset ${r.offset}`);
  console.log(`  motivo: ${r.motivo}`);
  console.log(`  valor:  ${String(r.valorOriginal).slice(0, 160)}\n`);
}
process.exit(0);
