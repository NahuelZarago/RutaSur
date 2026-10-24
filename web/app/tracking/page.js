import { redirect } from 'next/navigation';

export const dynamic = 'force-dynamic';

export default function BuscarEnvio({ searchParams }) {
  const codigo = (searchParams.codigo ?? '').trim();
  if (codigo) redirect(`/tracking/${encodeURIComponent(codigo.toUpperCase())}`);
  return (
    <section className="buscador">
      <h1>¿Dónde está mi paquete?</h1>
      <p>Ingresá el código que te mandó el comercio.</p>
      <form method="get" className="buscador-form">
        <label htmlFor="codigo" className="oculto">
          Código de envío
        </label>
        <input id="codigo" name="codigo" placeholder="RS-XXXX-00001" autoComplete="off" required />
        <button className="boton" type="submit">
          Buscar envío
        </button>
      </form>
    </section>
  );
}
