import { detalleEnvio } from '@/lib/consultas';

export const dynamic = 'force-dynamic';

export async function GET(_request, { params }) {
  const datos = await detalleEnvio(decodeURIComponent(params.id));
  if (!datos) return Response.json({ error: 'No existe un envío con ese código.' }, { status: 404 });
  return Response.json(datos);
}
