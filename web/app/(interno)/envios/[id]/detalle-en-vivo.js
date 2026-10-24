'use client';
import Link from 'next/link';
import { useEffect, useRef, useState } from 'react';
import {
  ETIQUETA_ESTADO,
  ETIQUETA_EVENTO,
  ETIQUETA_FRANJA,
  ETIQUETA_MOTIVO,
  ETIQUETA_PRIORIDAD,
  ETIQUETA_REGLA,
  dia,
  diaHora,
  nombreHub,
  nombreZona,
} from '@/lib/formato';

const ETIQUETA_REPROGRAMACION = { REPROGRAMADO: 'Reprogramado', RETENIDO: 'Retenido en hub', DEVOLUCION: 'Devolución al comercio' };

/** Une los cuatro orígenes (estados, reprogramaciones, notificaciones, alertas) en una línea de tiempo. */
function lineaDeTiempo({ envio, alertas, notificaciones }) {
  const items = [];
  envio.historial.forEach((h, i) =>
    items.push({
      clave: h.eventId,
      cuando: h.ocurridoEn,
      orden: i,
      clase: h.tipo === 'ENTREGA_FALLIDA' ? 'fallo' : h.tipo === 'ENTREGADO' ? 'ok' : 'estado',
      titulo: ETIQUETA_EVENTO[h.tipo] ?? h.tipo,
      detalle: [
        h.intento && `intento ${h.intento}`,
        h.franja && `franja ${ETIQUETA_FRANJA[h.franja].toLowerCase()}`,
        h.motivo && ETIQUETA_MOTIVO[h.motivo],
        h.repartidorId && `repartidor ${h.repartidorId}`,
      ].filter(Boolean),
      nota: h.demoraSincronizacionMin ? `Llegó con ${h.demoraSincronizacionMin} min de demora de sincronización` : null,
      origen: 'envios.estados',
    })
  );
  for (const r of envio.reprogramaciones) {
    items.push({
      clave: r.eventId,
      cuando: r.ocurridoEn,
      orden: 101,
      clase: 'derivado',
      titulo: ETIQUETA_REPROGRAMACION[r.tipo],
      detalle: [r.detalle],
      origen: 'envios.reprogramaciones',
    });
  }
  for (const n of notificaciones) {
    items.push({
      clave: n._id,
      cuando: n.ocurridoEn,
      orden: 102,
      clase: 'derivado',
      titulo: `Aviso al destinatario por ${n.canal === 'SMS' ? 'SMS' : 'correo'}`,
      detalle: [n.mensaje],
      origen: 'notificaciones.enviadas',
    });
  }
  for (const a of alertas) {
    items.push({
      clave: `${a._id}:a`,
      cuando: a.abiertaEn,
      orden: 100,
      clase: `alerta-${a.prioridad.toLowerCase()}`,
      titulo: `Alerta ${ETIQUETA_PRIORIDAD[a.prioridad].toLowerCase()}: ${ETIQUETA_REGLA[a.regla]}`,
      detalle: [a.causa],
      origen: 'alertas.sla',
    });
    if (a.estado === 'CERRADA' && a.cerradaEn) {
      items.push({ clave: `${a._id}:c`, cuando: a.cerradaEn, orden: 103, clase: 'ok', titulo: `Alerta cerrada: ${ETIQUETA_REGLA[a.regla]}`, detalle: ['El envío fue entregado.'], origen: 'alertas.sla' });
    }
  }
  return items.sort((a, b) => String(a.cuando).localeCompare(String(b.cuando)) || a.orden - b.orden);
}

export default function DetalleEnVivo({ id, inicial }) {
  const [datos, setDatos] = useState(inicial);
  const [enVivo, setEnVivo] = useState(null);
  const espera = useRef(null);

  useEffect(() => {
    const fuente = new EventSource(`/api/stream?shipmentId=${encodeURIComponent(id)}`);
    fuente.addEventListener('listo', () => setEnVivo(true));
    fuente.addEventListener('cambio', () => {
      clearTimeout(espera.current);
      espera.current = setTimeout(async () => {
        const r = await fetch(`/api/envios/${encodeURIComponent(id)}`, { cache: 'no-store' });
        if (r.ok) setDatos(await r.json());
      }, 150);
    });
    fuente.onerror = () => setEnVivo(false);
    return () => {
      clearTimeout(espera.current);
      fuente.close();
    };
  }, [id]);

  const { envio } = datos;
  const items = lineaDeTiempo(datos);
  const abiertas = datos.alertas.filter((a) => a.estado === 'ABIERTA');

  return (
    <div className="pagina detalle">
      <p className="miga">
        <Link href="/envios">Envíos</Link> / <span className="codigo">{envio._id}</span>
      </p>
      <header className="detalle-encabezado">
        <div>
          <h1 className="codigo-grande">{envio._id}</h1>
          <p>
            <span className={`etiqueta etiqueta-${envio.estadoVisible.toLowerCase()}`}>{ETIQUETA_ESTADO[envio.estadoVisible]}</span>
            <span className={`indicador-vivo ${enVivo ? 'vivo' : ''}`}>{enVivo ? 'Actualizando en vivo' : enVivo === null ? 'Conectando…' : 'Reconectando…'}</span>
          </p>
        </div>
        <Link className="boton boton-secundario" href={`/tracking/${envio._id}`}>
          Ver como destinatario
        </Link>
      </header>

      <dl className="ficha">
        <div><dt>Comercio</dt><dd>{envio.comercio ?? '—'}</dd></div>
        <div><dt>Zona</dt><dd>{nombreZona(envio.zonaId)}</dd></div>
        <div><dt>Hub</dt><dd>{nombreHub(envio.hubId)}</dd></div>
        <div><dt>Repartidor</dt><dd>{envio.repartidorId ?? 'Sin asignar'}</dd></div>
        <div><dt>Promesa</dt><dd>{diaHora(envio.promesa?.venceEn)}</dd></div>
        <div><dt>Intentos</dt><dd>{envio.intentos}</dd></div>
        {envio.resolucion?.proximoIntento && envio.estadoVisible === 'REPROGRAMADO' && (
          <div>
            <dt>Próximo intento</dt>
            <dd>
              {dia(envio.resolucion.proximoIntento.fecha)}, {ETIQUETA_FRANJA[envio.resolucion.proximoIntento.franja].toLowerCase()}
            </dd>
          </div>
        )}
        <div><dt>Avisos enviados</dt><dd>{envio.notificaciones.total}</dd></div>
      </dl>

      {abiertas.length > 0 && (
        <ul className="avisos-abiertos">
          {abiertas.map((a) => (
            <li key={a._id} className={`alerta prioridad-${a.prioridad.toLowerCase()}`}>
              <span className="alerta-prioridad">{ETIQUETA_PRIORIDAD[a.prioridad]}</span>
              <div className="alerta-cuerpo">
                <p className="alerta-titulo">{ETIQUETA_REGLA[a.regla]}</p>
                <p className="alerta-causa">{a.causa}</p>
              </div>
            </li>
          ))}
        </ul>
      )}

      <section aria-labelledby="titulo-linea">
        <h2 id="titulo-linea">Línea de tiempo</h2>
        <ol className="linea">
          {items.map((it) => (
            <li key={it.clave} className={`hito hito-${it.clase}`}>
              <span className="hito-cuando cifra">{diaHora(it.cuando)}</span>
              <div>
                <p className="hito-titulo">{it.titulo}</p>
                {it.detalle.length > 0 && <p className="hito-detalle">{it.detalle.join(', ')}</p>}
                {it.nota && <p className="hito-nota">{it.nota}</p>}
                <p className="hito-origen">{it.origen}</p>
              </div>
            </li>
          ))}
        </ol>
        {envio.descartadosFueraDeOrden > 0 && (
          <p className="tenue nota">
            Se descartaron {envio.descartadosFueraDeOrden} eventos con secuencia vieja (duplicados o fuera de orden); el estado no retrocedió.
          </p>
        )}
      </section>
    </div>
  );
}
