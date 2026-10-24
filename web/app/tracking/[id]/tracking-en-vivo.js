'use client';
import { useEffect, useRef, useState } from 'react';
import { diaHora, fechaEstimada, textoPublico } from '@/lib/formato';

const PASOS = [
  { tipo: 'CREADO', texto: 'Despachado' },
  { tipo: 'INGRESADO_HUB', texto: 'En el centro' },
  { tipo: 'EN_REPARTO', texto: 'En camino' },
  { tipo: 'ENTREGADO', texto: 'Entregado' },
];

export default function TrackingEnVivo({ id, inicial }) {
  const [envio, setEnvio] = useState(inicial);
  const espera = useRef(null);

  useEffect(() => {
    const fuente = new EventSource(`/api/stream?shipmentId=${encodeURIComponent(id)}`);
    fuente.addEventListener('cambio', () => {
      clearTimeout(espera.current);
      espera.current = setTimeout(async () => {
        const r = await fetch(`/api/tracking/${encodeURIComponent(id)}`, { cache: 'no-store' });
        if (r.ok) setEnvio(await r.json());
      }, 150);
    });
    return () => {
      clearTimeout(espera.current);
      fuente.close();
    };
  }, [id]);

  const { titulo, detalle } = textoPublico(envio);
  const estimada = fechaEstimada(envio);
  const vistos = new Set(envio.pasos.map((p) => (p.tipo === 'CLASIFICADO' ? 'INGRESADO_HUB' : p.tipo)));
  const desvio = ['REPROGRAMADO', 'RETENIDO', 'DEVOLUCION', 'ENTREGA_FALLIDA'].includes(envio.estadoVisible);

  return (
    <article className="tracking">
      <p className="tracking-codigo codigo">{envio.shipmentId}</p>
      <h1 className={desvio ? 'tracking-titulo desvio' : 'tracking-titulo'} aria-live="polite">
        {titulo}
      </h1>
      <p className="tracking-detalle">{detalle}</p>
      {estimada && (
        <p className="tracking-estimada">
          Fecha estimada: <strong>{estimada}</strong>
        </p>
      )}

      <ol className="tracking-pasos" aria-label="Progreso del envío">
        {PASOS.map((p) => (
          <li key={p.tipo} className={vistos.has(p.tipo) ? 'hecho' : undefined}>
            {p.texto}
          </li>
        ))}
      </ol>

      <h2>Novedades</h2>
      <ul className="tracking-novedades">
        {[...envio.pasos].reverse().map((p) => (
          <li key={p.eventId}>
            <span className="cifra">{diaHora(p.ocurridoEn)}</span>
            <span>{NOVEDAD[p.tipo]}</span>
          </li>
        ))}
      </ul>
      <p className="tracking-pie">
        {envio.comercio && <>Enviado por {envio.comercio}</>}
        {envio.localidad && <> a {envio.localidad}</>}
      </p>
    </article>
  );
}

const NOVEDAD = {
  CREADO: 'El comercio despachó tu envío',
  INGRESADO_HUB: 'Llegó a nuestro centro de distribución',
  CLASIFICADO: 'Se asignó a un repartidor de tu zona',
  EN_REPARTO: 'Salió a reparto',
  ENTREGADO: 'Entregado',
  ENTREGA_FALLIDA: 'Visitamos el domicilio pero no pudimos entregarlo',
};
