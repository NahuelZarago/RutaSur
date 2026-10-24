import Link from 'next/link';
import { trackingPublico } from '@/lib/consultas';
import TrackingEnVivo from './tracking-en-vivo';

export const dynamic = 'force-dynamic';

export default async function Tracking({ params }) {
  const id = decodeURIComponent(params.id);
  const datos = await trackingPublico(id);
  if (!datos) {
    return (
      <section className="buscador">
        <h1>No encontramos ese envío</h1>
        <p>
          Revisá el código <strong className="codigo">{id}</strong>. Si el comercio lo despachó hace instantes, puede tardar unos segundos en aparecer.
        </p>
        <Link className="boton" href="/tracking">
          Buscar otro código
        </Link>
      </section>
    );
  }
  return <TrackingEnVivo id={id} inicial={datos} />;
}
