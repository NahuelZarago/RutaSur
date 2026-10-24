'use client';
import Link from 'next/link';
import { useCallback, useEffect, useState } from 'react';
import { diaHora } from '@/lib/formato';

// Orden del guion de la demo; el resto de los escenarios queda en "Otros escenarios".
const GUION = ['feliz', 'ausente', 'demorado', 'malformado'];

const NOMBRE_GRUPO = {
  proyector: 'Proyector',
  notificaciones: 'Notificaciones',
  reprogramacion: 'Reprogramación',
  'monitor-sla': 'Monitor de SLA',
  analitica: 'Analítica',
  'analitica-tiempos-hub': 'Tiempos de hub (4.º indicador)',
};

async function pedir(ruta, metodo = 'GET') {
  const r = await fetch(`/api/demo/${ruta}`, { method: metodo, cache: 'no-store' });
  const cuerpo = await r.json();
  if (!r.ok) throw new Error(cuerpo.error ?? `Error ${r.status}`);
  return cuerpo;
}

export default function PanelDemo() {
  const [estado, setEstado] = useState(null);
  const [lag, setLag] = useState(null);
  const [error, setError] = useState(null);
  const [disparos, setDisparos] = useState([]);
  const [ocupado, setOcupado] = useState(null);

  const refrescar = useCallback(async () => {
    try {
      const [e, l] = await Promise.all([pedir('estado'), pedir('lag')]);
      setEstado(e);
      setLag(l);
      setError(null);
    } catch (e) {
      setError(e.message);
    }
  }, []);

  useEffect(() => {
    refrescar();
    const t = setInterval(refrescar, 2000);
    return () => clearInterval(t);
  }, [refrescar]);

  async function accion(ruta, etiqueta) {
    setOcupado(ruta);
    try {
      const r = await pedir(ruta, 'POST');
      if (r.escenario) setDisparos((d) => [{ ...r, etiqueta, en: Date.now() }, ...d].slice(0, 12));
      else setEstado(r);
      setError(null);
    } catch (e) {
      setError(e.message);
    } finally {
      setOcupado(null);
    }
  }

  const escenarios = estado?.escenarios ?? [];
  const delGuion = GUION.map((id) => escenarios.find((e) => e.id === id)).filter(Boolean);
  const otros = escenarios.filter((e) => !GUION.includes(e.id));

  return (
    <div className="pagina demo">
      <header className="demo-encabezado">
        <div>
          <h1>Panel de demo</h1>
          <p className="tenue">Dispara escenarios del simulador y muestra el lag de cada grupo de consumidores.</p>
        </div>
        {estado && (
          <div className="reloj-demo">
            <p className="reloj-cifra">{diaHora(estado.relojSimulado)}</p>
            <p className="tenue">
              Corrida {estado.corrida}, semilla {estado.semilla}, 1 h simulada = {Math.round(3600 / estado.factor)} s
            </p>
            <div className="botonera">
              {estado.pausado ? (
                <button className="boton" disabled={!!ocupado} onClick={() => accion('reanudar')}>Reanudar el reloj</button>
              ) : (
                <button className="boton boton-secundario" disabled={!!ocupado} onClick={() => accion('pausar')}>Pausar el reloj</button>
              )}
              {estado.flujoActivo ? (
                <button className="boton boton-secundario" disabled={!!ocupado} onClick={() => accion('flujo/detener')}>Detener el flujo continuo</button>
              ) : (
                <button className="boton" disabled={!!ocupado} onClick={() => accion('flujo/reanudar')}>Reanudar el flujo continuo</button>
              )}
            </div>
          </div>
        )}
      </header>

      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}

      <section aria-labelledby="titulo-guion">
        <h2 id="titulo-guion">Escenarios del guion</h2>
        <ul className="escenarios">
          {delGuion.map((e) => (
            <li key={e.id}>
              <button className="escenario" disabled={!!ocupado} onClick={() => accion(`escenarios/${e.id}`, e.titulo)}>
                <span className="escenario-titulo">{e.titulo}</span>
                <span className="escenario-texto">{e.descripcion}</span>
              </button>
            </li>
          ))}
        </ul>
        <details className="otros">
          <summary>Otros escenarios ({otros.length})</summary>
          <ul className="escenarios escenarios-chicos">
            {otros.map((e) => (
              <li key={e.id}>
                <button className="escenario" disabled={!!ocupado} onClick={() => accion(`escenarios/${e.id}`, e.titulo)}>
                  <span className="escenario-titulo">{e.titulo}</span>
                  <span className="escenario-texto">{e.descripcion}</span>
                </button>
              </li>
            ))}
          </ul>
        </details>
      </section>

      {disparos.length > 0 && (
        <section aria-labelledby="titulo-disparos">
          <h2 id="titulo-disparos">Envíos creados</h2>
          <ul className="disparos">
            {disparos.map((d) => (
              <li key={d.en}>
                <strong>{d.etiqueta}</strong>
                {d.shipmentIds.length === 0 && d.detalle?.envios && <span> {d.detalle.envios} envíos en camino</span>}
                {d.shipmentIds.map((id) => (
                  <span key={id} className="disparo-enlaces">
                    <span className="codigo">{id}</span>
                    {d.escenario === 'malformado' ? (
                      <span className="tenue">va a la DLQ ({d.detalle?.clase})</span>
                    ) : (
                      <>
                        <Link href={`/envios/${id}`}>Detalle</Link>
                        <Link href={`/tracking/${id}`} target="_blank">Tracking público</Link>
                      </>
                    )}
                  </span>
                ))}
              </li>
            ))}
          </ul>
        </section>
      )}

      <section aria-labelledby="titulo-lag">
        <h2 id="titulo-lag">Grupos de consumidores</h2>
        {lag ? (
          <>
            <table className="tabla">
              <thead>
                <tr>
                  <th scope="col">Grupo</th>
                  <th scope="col">Estado</th>
                  <th scope="col" className="cifra">Instancias</th>
                  <th scope="col" className="cifra">Lag</th>
                </tr>
              </thead>
              <tbody>
                {lag.grupos.map((g) => {
                  const detenido = g.iniciado && g.miembros === 0;
                  return (
                    <tr key={g.groupId} className={detenido ? 'fila-detenida' : undefined}>
                      <th scope="row">
                        {NOMBRE_GRUPO[g.groupId] ?? g.groupId} <span className="tenue codigo">{g.groupId}</span>
                      </th>
                      <td>{!g.iniciado ? 'Sin activar' : detenido ? 'Detenido' : 'Consumiendo'}</td>
                      <td className="cifra">{g.miembros}</td>
                      <td className={`cifra ${g.lagTotal > 0 ? 'lag-alto' : ''}`}>{g.iniciado ? g.lagTotal.toLocaleString('es-AR') : '—'}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            <p className="dlq">
              Mensajes en la DLQ: <strong>{lag.dlq}</strong>
            </p>
          </>
        ) : (
          <p className="tenue">Consultando…</p>
        )}
      </section>

      <section aria-labelledby="titulo-comandos" className="comandos">
        <h2 id="titulo-comandos">Comandos para los pasos con fallos provocados</h2>
        <dl>
          <dt>Caída de un servicio</dt>
          <dd>
            <pre>docker compose stop notificaciones</pre>
            <p>El lag del grupo crece y el resto sigue funcionando. Para retomar desde el último offset:</p>
            <pre>docker compose start notificaciones</pre>
          </dd>
          <dt>Revisar la DLQ</dt>
          <dd>
            <pre>docker compose run --rm herramientas node scripts/dlq.js</pre>
          </dd>
          <dt>Activar el cuarto indicador (reprocesamiento)</dt>
          <dd>
            <pre>docker compose --profile replay up -d analitica-tiempos</pre>
          </dd>
          <dt>Reconstruir la vista de lectura desde Kafka</dt>
          <dd>
            <pre>bash scripts/reconstruccion.sh</pre>
          </dd>
        </dl>
      </section>
    </div>
  );
}
