// Server-Sent Events alimentados por change streams de MongoDB (envios y alertas).
// El frontend nunca habla con Kafka: el flujo es Kafka → proyector → MongoDB → change stream → SSE.
//
//   /api/stream                 → canal de la torre de control (contadores, eventos, alertas)
//   /api/stream?shipmentId=ID   → avisa cuando cambia un envío (detalle y tracking público)
import { obtenerDb } from '@/lib/mongo';
import { contadores, resumenAlerta, resumenEnvio } from '@/lib/consultas';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

const INTERVALO_LOTE_MS = 300;

export async function GET(request) {
  const shipmentId = new URL(request.url).searchParams.get('shipmentId');
  const db = await obtenerDb();
  const codificador = new TextEncoder();
  const cursores = [];
  const temporizadores = [];
  let cerrado = false;
  let cerrar = () => {};

  const stream = new ReadableStream({
    start(controller) {
      const escribir = (texto) => {
        if (cerrado) return;
        try {
          controller.enqueue(codificador.encode(texto));
        } catch {
          cerrar();
        }
      };
      const enviar = (evento, datos) => escribir(`event: ${evento}\ndata: ${JSON.stringify(datos)}\n\n`);

      cerrar = () => {
        if (cerrado) return;
        cerrado = true;
        temporizadores.forEach(clearInterval);
        cursores.forEach((c) => c.close().catch(() => {}));
        try {
          controller.close();
        } catch {
          /* ya cerrado */
        }
      };
      request.signal.addEventListener('abort', cerrar);

      const vigilar = (coleccion, pipeline, alCambiar) => {
        const cursor = db.collection(coleccion).watch(pipeline, { fullDocument: 'updateLookup' });
        cursor.on('change', (cambio) => {
          try {
            alCambiar(cambio);
          } catch {
            /* un cambio mal formado no corta el stream */
          }
        });
        cursor.on('error', () => {
          enviar('error', { mensaje: 'Se cortó el change stream; el navegador va a reconectar.' });
          cerrar();
        });
        cursores.push(cursor);
      };

      enviar('listo', { ts: Date.now() });
      temporizadores.push(setInterval(() => escribir(': ping\n\n'), 15_000));

      if (shipmentId) {
        vigilar('envios', [{ $match: { 'documentKey._id': shipmentId } }], () => enviar('cambio', { shipmentId }));
        vigilar('alertas', [], (c) => {
          if (c.fullDocument?.shipmentId === shipmentId) enviar('cambio', { shipmentId });
        });
        return;
      }

      // Canal de la torre: agrupa cambios en lotes cortos para no saturar el navegador en una ráfaga.
      let lote = new Map();
      let contadoresSucios = false;
      vigilar('envios', [{ $match: { operationType: { $in: ['insert', 'update', 'replace'] } } }], (c) => {
        const doc = c.fullDocument;
        if (!doc) return;
        contadoresSucios = true;
        if (doc.ultimoEvento?.eventId) lote.set(doc.ultimoEvento.eventId, resumenEnvio(doc));
      });
      vigilar('alertas', [{ $match: { operationType: { $in: ['insert', 'update', 'replace'] } } }], (c) => {
        if (c.fullDocument) enviar('alerta', resumenAlerta(c.fullDocument));
      });
      temporizadores.push(
        setInterval(async () => {
          if (lote.size) {
            enviar('envios', [...lote.values()]);
            lote = new Map();
          }
          if (contadoresSucios) {
            contadoresSucios = false;
            try {
              enviar('contadores', await contadores());
            } catch {
              contadoresSucios = true;
            }
          }
        }, INTERVALO_LOTE_MS)
      );
    },
    cancel() {
      cerrar();
    },
  });

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    },
  });
}
