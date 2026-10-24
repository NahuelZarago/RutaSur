// Conexión única a MongoDB, reutilizada entre requests (y entre recargas en desarrollo).
// La interfaz solo lee: nunca escribe ni consume Kafka.
import { MongoClient } from 'mongodb';

const url = process.env.MONGO_URL ?? 'mongodb://localhost:27017/?directConnection=true';
const nombreDb = process.env.MONGO_DB ?? 'rutasur';

export async function obtenerDb() {
  if (!globalThis.__rutasurMongo) {
    globalThis.__rutasurMongo = new MongoClient(url, { maxPoolSize: 50 }).connect();
  }
  try {
    const client = await globalThis.__rutasurMongo;
    return client.db(nombreDb);
  } catch (e) {
    globalThis.__rutasurMongo = undefined;
    throw e;
  }
}
