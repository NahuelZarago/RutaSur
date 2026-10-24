// Proxy del panel de demo hacia la API del simulador (no es parte del flujo de datos de producto).
export const dynamic = 'force-dynamic';

const BASE = process.env.SIMULADOR_URL ?? 'http://localhost:4000';
const PERMITIDAS = /^(estado|lag|pausar|reanudar|flujo\/(detener|reanudar)|escenarios\/[a-z-]+)$/;

async function reenviar(request, params, metodo) {
  const ruta = params.ruta.join('/');
  if (!PERMITIDAS.test(ruta)) return Response.json({ error: 'Ruta no permitida' }, { status: 404 });
  try {
    const respuesta = await fetch(`${BASE}/${ruta}`, {
      method: metodo,
      cache: 'no-store',
      headers: { 'content-type': 'application/json' },
      body: metodo === 'POST' ? (await request.text()) || '{}' : undefined,
    });
    return new Response(await respuesta.text(), {
      status: respuesta.status,
      headers: { 'content-type': 'application/json; charset=utf-8' },
    });
  } catch {
    return Response.json({ error: `El simulador no responde en ${BASE}. Revisá que el contenedor esté levantado.` }, { status: 502 });
  }
}

export async function GET(request, { params }) {
  return reenviar(request, params, 'GET');
}

export async function POST(request, { params }) {
  return reenviar(request, params, 'POST');
}
