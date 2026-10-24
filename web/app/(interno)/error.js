'use client';

export default function ErrorInterno({ error, reset }) {
  return (
    <section className="vacio">
      <h1>No se pudo leer MongoDB</h1>
      <p>
        La interfaz lee las vistas que escribe el proyector. Revisá que el contenedor <code>mongo</code> esté sano con{' '}
        <code>docker compose ps</code> y volvé a intentar.
      </p>
      <p className="tenue">{error?.message}</p>
      <button className="boton" onClick={() => reset()}>
        Reintentar
      </button>
    </section>
  );
}
