'use client';
import Link from 'next/link';
import { useEffect, useMemo, useState } from 'react';
import {
  ESTADOS_DESVIO,
  ESTADOS_RECORRIDO,
  ETIQUETA_ESTADO,
  ETIQUETA_EVENTO,
  ETIQUETA_MOTIVO,
  ETIQUETA_PRIORIDAD,
  ETIQUETA_REGLA,
  diaHora,
  hora,
  nombreZona,
  ordenarAlertas,
} from '@/lib/formato';

const MAX_EVENTOS = 15;

function mediana(valores) {
  if (!valores.length) return null;
  const v = [...valores].sort((a, b) => a - b);
  return v[Math.floor(v.length / 2)];
}

export default function TorreEnVivo({ inicial }) {
  const [contadores, setContadores] = useState(inicial.contadores);
  const [alertas, setAlertas] = useState(inicial.alertas);
  const [eventos, setEventos] = useState(inicial.eventos);
  const [reloj, setReloj] = useState(inicial.reloj);
  const [conexion, setConexion] = useState('conectando');
  const [latencias, setLatencias] = useState([]);
  const [nuevos, setNuevos] = useState(new Set());

  useEffect(() => {
    const fuente = new EventSource('/api/stream');
    fuente.addEventListener('listo', () => setConexion('en vivo'));
    fuente.addEventListener('contadores', (e) => setContadores(JSON.parse(e.data)));
    fuente.addEventListener('envios', (e) => {
      const lote = JSON.parse(e.data);
      const ahora = Date.now();
      setLatencias((prev) => [...lote.filter((x) => x.kafkaTs).map((x) => ahora - x.kafkaTs), ...prev].slice(0, 200));
      setEventos((prev) => {
        const porId = new Map(prev.map((x) => [x.eventId, x]));
        for (const x of lote) porId.set(x.eventId, x);
        return [...porId.values()].sort((a, b) => (b.kafkaTs ?? 0) - (a.kafkaTs ?? 0)).slice(0, MAX_EVENTOS);
      });
      setNuevos(new Set(lote.map((x) => x.eventId)));
      const maximo = lote.reduce((m, x) => (x.ocurridoEn && x.ocurridoEn > m ? x.ocurridoEn : m), '');
      if (maximo) setReloj((r) => (!r || maximo > r ? maximo : r));
    });
    fuente.addEventListener('alerta', (e) => {
      const alerta = JSON.parse(e.data);
      setAlertas((prev) => {
        const resto = prev.filter((a) => a._id !== alerta._id);
        return ordenarAlertas(alerta.estado === 'ABIERTA' ? [...resto, alerta] : resto).slice(0, 100);
      });
    });
    fuente.onerror = () => setConexion('reconectando');
    return () => fuente.close();
  }, []);

  const total = useMemo(() => Object.values(contadores).reduce((s, n) => s + n, 0), [contadores]);
  const latenciaMediana = mediana(latencias);
  const latenciaMaxima = latencias.length ? Math.max(...latencias) : null;
  const criticas = alertas.filter((a) => a.prioridad === 'CRITICA').length;

  return (
    <div className="torre">
      <section className="encabezado-torre">
        <div>
          <h1>Torre de control</h1>
          <p className="tenue">
            {total.toLocaleString('es-AR')} envíos en seguimiento. Reloj simulado: <strong>{diaHora(reloj)}</strong>
          </p>
        </div>
        <dl className="pulso" aria-live="polite">
          <div>
            <dt>Conexión</dt>
            <dd className={`estado-conexion ${conexion === 'en vivo' ? 'vivo' : ''}`}>{conexion}</dd>
          </div>
          <div>
            <dt>Evento → pantalla</dt>
            <dd title="Mediana y máximo de las últimas 200 actualizaciones (requisito: menos de 2 s)">
              {latenciaMediana === null ? '—' : `${latenciaMediana} ms`}
              {latenciaMaxima !== null && <span className="tenue"> / máx {latenciaMaxima} ms</span>}
            </dd>
          </div>
        </dl>
      </section>

      <section className="recorrido" aria-label="Envíos por estado">
        <ol className="paradas">
          {ESTADOS_RECORRIDO.map((estado) => (
            <li key={estado} className={`parada parada-${estado.toLowerCase()}`}>
              <Link href={`/envios?estado=${estado}`}>
                <span className="parada-cifra">{(contadores[estado] ?? 0).toLocaleString('es-AR')}</span>
                <span className="parada-nombre">{ETIQUETA_ESTADO[estado]}</span>
              </Link>
            </li>
          ))}
        </ol>
        <ul className="desvios" aria-label="Envíos fuera del recorrido normal">
          {ESTADOS_DESVIO.map((estado) => (
            <li key={estado}>
              <Link href={`/envios?estado=${estado}`}>
                <span className={`punto punto-${estado.toLowerCase()}`} aria-hidden="true" />
                {ETIQUETA_ESTADO[estado]} <strong>{(contadores[estado] ?? 0).toLocaleString('es-AR')}</strong>
              </Link>
            </li>
          ))}
        </ul>
      </section>

      <div className="torre-cuerpo">
        <section className="panel panel-alertas" aria-labelledby="titulo-alertas">
          <header className="panel-encabezado">
            <h2 id="titulo-alertas">Alertas abiertas</h2>
            <span className="tenue">
              {alertas.length} {criticas > 0 && <strong className="texto-critico">({criticas} críticas)</strong>}
            </span>
          </header>
          {alertas.length === 0 ? (
            <p className="vacio-panel">Sin envíos en riesgo. Cuando un envío se acerque a su vencimiento, aparece acá.</p>
          ) : (
            <ul className="cola">
              {alertas.map((a) => (
                <li key={a._id} className={`alerta prioridad-${a.prioridad.toLowerCase()}`}>
                  <span className="alerta-prioridad">{ETIQUETA_PRIORIDAD[a.prioridad]}</span>
                  <div className="alerta-cuerpo">
                    <p className="alerta-titulo">
                      {ETIQUETA_REGLA[a.regla]}{' '}
                      <Link href={`/envios/${a.shipmentId}`} className="codigo">
                        {a.shipmentId}
                      </Link>
                    </p>
                    <p className="alerta-causa">{a.causa}</p>
                  </div>
                  <div className="alerta-meta">
                    <span>{nombreZona(a.zonaId)}</span>
                    <span className="tenue">vence {diaHora(a.venceEn)}</span>
                  </div>
                </li>
              ))}
            </ul>
          )}
        </section>

        <section className="panel panel-eventos" aria-labelledby="titulo-eventos">
          <header className="panel-encabezado">
            <h2 id="titulo-eventos">Últimos eventos</h2>
          </header>
          <table className="feed">
            <thead>
              <tr>
                <th scope="col">Hora</th>
                <th scope="col">Envío</th>
                <th scope="col">Evento</th>
              </tr>
            </thead>
            <tbody>
              {eventos.map((e) => (
                <tr key={e.eventId} className={nuevos.has(e.eventId) ? 'recien' : undefined}>
                  <td className="cifra">{hora(e.ocurridoEn)}</td>
                  <td>
                    <Link href={`/envios/${e.shipmentId}`} className="codigo">
                      {e.shipmentId}
                    </Link>
                    <span className="tenue fila-zona">{nombreZona(e.zonaId)}</span>
                  </td>
                  <td>
                    <span className={`punto punto-${(e.tipo === 'ENTREGA_FALLIDA' ? 'entrega_fallida' : e.tipo ?? '').toLowerCase()}`} aria-hidden="true" />
                    {ETIQUETA_EVENTO[e.tipo] ?? e.tipo}
                    {e.motivo && <span className="tenue"> ({ETIQUETA_MOTIVO[e.motivo]})</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      </div>
    </div>
  );
}
