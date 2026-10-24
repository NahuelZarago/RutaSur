import { datosTorre } from '@/lib/consultas';
import TorreEnVivo from './torre-en-vivo';

export const dynamic = 'force-dynamic';

export default async function TorreDeControl() {
  const inicial = await datosTorre();
  return <TorreEnVivo inicial={inicial} />;
}
