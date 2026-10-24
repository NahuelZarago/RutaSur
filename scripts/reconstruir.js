// Criterio de aceptación: "borrar envios y reprocesar deja la colección en el mismo estado".
//
// Tres pasos (los orquesta scripts/reconstruccion.sh):
//   instantanea  pausa el simulador, espera lag 0 del proyector y guarda un hash de las vistas
//   reiniciar    (con el proyector detenido) borra las vistas y sus marcas de idempotencia y
//                rebobina los offsets del grupo proyector al inicio de cada topic
//   comparar     espera a que el proyector reprocese todo, recalcula el hash y lo compara
import { createHash } from 'node:crypto';
import { config } from '../shared/config.js';
import { GRUPOS, SUSCRIPCIONES } from '../shared/eventos.js';
import { crearKafka } from '../shared/kafka.js';
import { conectarMongo } from '../shared/mongo.js';
import { espera, esperarLagCero, llamarSimulador } from './comun.js';

const VISTAS = ['envios', 'alertas', 'notificaciones'];
const paso = process.argv[2];
const kafka = crearKafka('herramientas-reconstruccion');

function canonico(valor) {
  if (Array.isArray(valor)) return `[${valor.map(canonico).join(',')}]`;
  if (valor && typeof valor === 'object') {
    return `{${Object.keys(valor).sort().map((k) => `${JSON.stringify(k)}:${canonico(valor[k])}`).join(',')}}`;
  }
  return JSON.stringify(valor);
}

async function huella(db) {
  const resultado = {};
  for (const nombre of VISTAS) {
    const hash = createHash('sha256');
    let n = 0;
    for await (const doc of db.collection(nombre).find({}).sort({ _id: 1 })) {
      hash.update(canonico(doc));
      n++;
    }
    resultado[nombre] = { documentos: n, sha256: hash.digest('hex') };
  }
  return resultado;
}

const { client, db } = await conectarMongo();
let codigo = 0;

if (paso === 'instantanea') {
  const pausado = await llamarSimulador(config.simuladorUrl, 'pausar');
  console.log(pausado ? 'Simulador pausado.' : 'Aviso: no se pudo pausar el simulador (¿está corriendo?).');
  await espera(3000); // deja que monitor y notificaciones terminen lo que tengan en curso
  await esperarLagCero(kafka, GRUPOS.PROYECTOR);
  await espera(1500);
  await esperarLagCero(kafka, GRUPOS.PROYECTOR);
  const h = await huella(db);
  await db.collection('reconstrucciones').replaceOne({ _id: 'ultima' }, { _id: 'ultima', huella: h, tomadaEn: new Date() }, { upsert: true });
  console.table(h);
  console.log('Instantánea guardada.');
} else if (paso === 'reiniciar') {
  const admin = kafka.admin();
  await admin.connect();
  const [grupo] = (await admin.describeGroups([GRUPOS.PROYECTOR])).groups;
  if (grupo.members.length > 0) {
    console.error('El proyector sigue conectado. Detenelo primero: docker compose stop proyector');
    codigo = 1;
  } else {
    for (const nombre of VISTAS) await db.collection(nombre).deleteMany({});
    const { deletedCount } = await db.collection('eventos_procesados').deleteMany({ consumidor: 'proyector' });
    for (const topic of SUSCRIPCIONES[GRUPOS.PROYECTOR]) {
      await admin.resetOffsets({ groupId: GRUPOS.PROYECTOR, topic, earliest: true });
    }
    console.log(`Vistas borradas (${VISTAS.join(', ')}), ${deletedCount} marcas de idempotencia eliminadas.`);
    console.log('Offsets del grupo "proyector" rebobinados al inicio. Ahora: docker compose start proyector');
  }
  await admin.disconnect();
} else if (paso === 'comparar') {
  await espera(3000);
  await esperarLagCero(kafka, GRUPOS.PROYECTOR, { maxMs: 600_000 });
  const despues = await huella(db);
  const guardada = await db.collection('reconstrucciones').findOne({ _id: 'ultima' });
  if (!guardada) {
    console.error('No hay instantánea previa. Corré primero el paso "instantanea".');
    codigo = 1;
  } else {
    const filas = {};
    for (const nombre of VISTAS) {
      filas[nombre] = {
        antes: `${guardada.huella[nombre].documentos} docs · ${guardada.huella[nombre].sha256.slice(0, 12)}`,
        despues: `${despues[nombre].documentos} docs · ${despues[nombre].sha256.slice(0, 12)}`,
        iguales: guardada.huella[nombre].sha256 === despues[nombre].sha256 ? 'sí' : 'NO',
      };
    }
    console.table(filas);
    const iguales = VISTAS.every((n) => guardada.huella[n].sha256 === despues[n].sha256);
    console.log(iguales ? '✓ La reconstrucción desde Kafka dejó las vistas idénticas.' : '✗ Las vistas difieren.');
    codigo = iguales ? 0 : 1;
  }
  if (!process.argv.includes('--sin-reanudar')) await llamarSimulador(config.simuladorUrl, 'reanudar');
} else {
  console.error('Uso: node scripts/reconstruir.js <instantanea|reiniciar|comparar>');
  codigo = 1;
}

await client.close();
process.exit(codigo);
