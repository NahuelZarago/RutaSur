// Configuración por variables de entorno. Los valores por defecto sirven para correr un servicio
// desde la máquina anfitriona contra el docker-compose (Kafka en 29092, Mongo en 27017).

const env = process.env;

export const config = {
  kafkaBrokers: (env.KAFKA_BROKERS ?? 'localhost:29092').split(',').map((s) => s.trim()),
  mongoUrl: env.MONGO_URL ?? 'mongodb://localhost:27017/?directConnection=true',
  mongoDb: env.MONGO_DB ?? 'rutasur',
  simuladorUrl: env.SIMULADOR_URL ?? 'http://localhost:4000',
  consumoConcurrente: Number(env.CONSUMO_CONCURRENTE ?? 3),
  logNivel: env.LOG_NIVEL ?? 'info',
};
