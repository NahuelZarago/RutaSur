import { notFound } from 'next/navigation';
import { detalleEnvio } from '@/lib/consultas';
import DetalleEnVivo from './detalle-en-vivo';

export const dynamic = 'force-dynamic';

export default async function DetalleEnvio({ params }) {
  const id = decodeURIComponent(params.id);
  const datos = await detalleEnvio(id);
  if (!datos) notFound();
  return <DetalleEnVivo id={id} inicial={JSON.parse(JSON.stringify(datos))} />;
}
