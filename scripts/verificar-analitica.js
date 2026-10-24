// Criterio de aceptación: "los tres indicadores coinciden con un recuento manual sobre una corrida
// con semilla fija". Lee los eventos crudos de Kafka, recuenta los indicadores con un camino de
// cálculo independiente y los compara con metricas_diarias.
//
//   docker compose run --rm herramientas node scripts/verificar-analitica.js
import { decodificarMensaje } from '../shared/esquemas.js';
import { GRUPOS, TOPICS } from '../shared/eventos.js';
import { crearKafka } from '../shared/kafka.js';
import { conectarMongo } from '../shared/mongo.js';
import { compararIndicadores, indicadoresDesdeMetricas, recontarIndicadores } from '../shared/dominio/recuento.js';
import { config } from '../shared/config.js';
import { espera, esperarLagCero, leerTopicCompleto, llamarSimulador } from './comun.js';

const kafka = crearKafka('herramientas-verificacion');
const pausado = await llamarSimulador(config.simuladorUrl, 'pausar');
console.log(pausado ? '0. Simulador pausado para tomar una foto consistente.' : '0. Simulador no disponible: se verifica lo que haya en el log.');
await espera(1500);
console.log('1. Esperando a que la analítica procese todo lo publicado…');
await esperarLagCero(kafka, GRUPOS.ANALITICA);

console.log('2. Leyendo envios.estados y envios.promesas desde el inicio…');
const crudos = await leerTopicCompleto(kafka, [TOPICS.ESTADOS, TOPICS.PROMESAS]);
const estados = [];
const promesas = [];
let invalidos = 0;
for (const m of crudos) {
  const d = decodificarMensaje(m.topic, m.key, m.value);
  if (!d.ok) {
    invalidos++;
    continue;
  }
  (m.topic === TOPICS.ESTADOS ? estados : promesas).push(d.evento);
}
console.log(`   ${estados.length} eventos de estado, ${promesas.length} promesas, ${invalidos} inválidos (DLQ)`);

console.log('3. Recuento manual y comparación con metricas_diarias…');
const esperado = recontarIndicadores(estados, promesas);
const { client, db } = await conectarMongo();
const obtenido = indicadoresDesdeMetricas(await db.collection('metricas_diarias').find({}).toArray());
await client.close();

if (pausado && !process.argv.includes('--sin-reanudar')) await llamarSimulador(config.simuladorUrl, 'reanudar');

const diferencias = compararIndicadores(esperado, obtenido);
const resumen = (r) => ({
  celdasPrimerIntento: Object.keys(r.primerIntento).length,
  intentosPrimerIntento: Object.values(r.primerIntento).reduce((s, c) => s + c.total, 0),
  fallosPorMotivo: Object.values(r.motivos).reduce((s, c) => s + Object.values(c).reduce((a, b) => a + b, 0), 0),
  entregasConPromesa: Object.values(r.sla).reduce((s, c) => s + c.conPromesa, 0),
});
console.table({ recuento: resumen(esperado), metricas: resumen(obtenido) });

if (diferencias.length) {
  console.error(`\n✗ ${diferencias.length} diferencias:`);
  for (const d of diferencias.slice(0, 30)) console.error('  ' + d);
  process.exit(1);
}
console.log('\n✓ Los tres indicadores coinciden con el recuento manual.');
process.exit(0);
