import { datosAnalitica } from '@/lib/consultas';
import { ETIQUETA_FRANJA, ETIQUETA_MOTIVO, dia, hubs, nombreHub, nombreZona, porcentaje, zonas } from '@/lib/formato';

export const dynamic = 'force-dynamic';

const MOTIVOS = ['AUSENTE', 'DIRECCION_INCORRECTA', 'RECHAZADO', 'ZONA_INACCESIBLE'];
const FRANJAS = ['MANANA', 'TARDE'];
const MUESTRA_MINIMA = 20; // celdas con pocos intentos no alcanzan para sacar conclusiones

/** Color de una celda según la tasa de fallo: más fallo, más intenso. */
function tonoFallo(tasa) {
  if (tasa === null) return undefined;
  const t = Math.min(1, tasa / 0.35);
  return { '--intensidad': t.toFixed(2) };
}

export default async function Analitica({ searchParams }) {
  const filtros = {
    hub: searchParams.hub ?? '',
    zona: searchParams.zona ?? '',
    desde: searchParams.desde ?? '',
    hasta: searchParams.hasta ?? '',
  };
  const d = await datosAnalitica(filtros);

  // ---- indicador 1: primer intento por zona × franja
  const celdas = new Map(d.zonaFranja.map((f) => [`${f._id.zonaId}|${f._id.franja}`, f]));
  const zonasVisibles = zonas.filter((z) => (!filtros.zona || z.id === filtros.zona) && (!filtros.hub || z.hubId === filtros.hub));
  const totalPI = d.zonaFranja.reduce((s, f) => s + f.total, 0);
  const exitososPI = d.zonaFranja.reduce((s, f) => s + f.exitosos, 0);
  const peor = d.zonaFranja
    .filter((f) => f.total >= MUESTRA_MINIMA)
    .map((f) => ({ ...f._id, total: f.total, tasaFallo: 1 - f.exitosos / f.total }))
    .sort((a, b) => b.tasaFallo - a.tasaFallo || b.total - a.total)[0];

  // ---- indicador 2: motivos
  const motivosPorZona = new Map(d.motivos.map((m) => [m._id, m]));
  const totalMotivos = Object.fromEntries(MOTIVOS.map((k) => [k, d.motivos.reduce((s, m) => s + (m[k] ?? 0), 0)]));
  const totalFallos = Object.values(totalMotivos).reduce((s, n) => s + n, 0);
  const motivoPeor = peor && motivosPorZona.get(peor.zonaId);
  const motivoPrincipalPeor = motivoPeor ? MOTIVOS.reduce((a, b) => ((motivoPeor[b] ?? 0) > (motivoPeor[a] ?? 0) ? b : a)) : null;

  // ---- indicador 3: SLA por hub y día
  const fechasSla = [...new Set(d.sla.map((s) => s._id.fecha))].sort();
  const sla = new Map(d.sla.map((s) => [`${s._id.hubId}|${s._id.fecha}`, s]));
  const hubsVisibles = hubs.filter((h) => !filtros.hub || h.id === filtros.hub);
  const slaHub = (hubId) => {
    const filas = d.sla.filter((s) => s._id.hubId === hubId);
    const c = filas.reduce((a, s) => a + s.conPromesa, 0);
    return { conPromesa: c, enPlazo: filas.reduce((a, s) => a + s.enPlazo, 0) };
  };
  const peorHub = hubsVisibles
    .map((h) => ({ ...h, ...slaHub(h.id) }))
    .filter((h) => h.conPromesa > 0)
    .sort((a, b) => a.enPlazo / a.conPromesa - b.enPlazo / b.conPromesa)[0];

  // ---- indicador 4: tiempo en hub
  const tiemposPorHub = hubsVisibles.map((h) => {
    const filas = d.tiempos.filter((t) => t._id.hubId === h.id);
    const suma = filas.reduce((s, t) => s + t.sumaMinutos, 0);
    const n = filas.reduce((s, t) => s + t.cantidad, 0);
    return { ...h, n, media: n ? suma / n : null, porDia: filas };
  });
  const maxTiempo = Math.max(1, ...tiemposPorHub.map((t) => t.media ?? 0));

  const sinDatos = totalPI === 0 && totalFallos === 0;

  return (
    <div className="pagina analitica">
      <header className="analitica-encabezado">
        <h1>Analítica de mejora</h1>
        <form className="filtros" method="get">
          <label>
            Hub
            <select name="hub" defaultValue={filtros.hub}>
              <option value="">Todos</option>
              {hubs.map((h) => (
                <option key={h.id} value={h.id}>{h.nombre}</option>
              ))}
            </select>
          </label>
          <label>
            Zona
            <select name="zona" defaultValue={filtros.zona}>
              <option value="">Todas</option>
              {zonas.map((z) => (
                <option key={z.id} value={z.id}>{z.nombre}</option>
              ))}
            </select>
          </label>
          <label>
            Desde
            <select name="desde" defaultValue={filtros.desde}>
              <option value="">Inicio</option>
              {d.fechas.map((f) => (
                <option key={f} value={f}>{dia(f)}</option>
              ))}
            </select>
          </label>
          <label>
            Hasta
            <select name="hasta" defaultValue={filtros.hasta}>
              <option value="">Hoy</option>
              {d.fechas.map((f) => (
                <option key={f} value={f}>{dia(f)}</option>
              ))}
            </select>
          </label>
          <button className="boton" type="submit">Aplicar</button>
        </form>
      </header>

      {sinDatos ? (
        <p className="vacio-panel">Todavía no hay entregas resueltas en este período. Los indicadores se llenan a medida que llegan ENTREGADO y ENTREGA_FALLIDA.</p>
      ) : (
        <>
          {peor && (
            <section className="hallazgo" aria-label="Hallazgo principal">
              <p className="hallazgo-cifra">{Math.round(peor.tasaFallo * 100)} %</p>
              <div>
                <h2>
                  {nombreZona(peor.zonaId)}, franja {ETIQUETA_FRANJA[peor.franja].toLowerCase()}, es donde más fallamos en el primer intento
                </h2>
                <p>
                  {peor.total} primeros intentos; motivo principal: {ETIQUETA_MOTIVO[motivoPrincipalPeor]?.toLowerCase()}.
                  {motivoPrincipalPeor === 'AUSENTE' && peor.franja === 'MANANA'
                    ? ' Mejora propuesta: ofrecer una franja vespertina (19 a 22 h) en esta zona, cuando hay alguien en el domicilio.'
                    : ' Mejora propuesta: revisar la planificación de esta franja con el equipo del hub.'}
                </p>
              </div>
            </section>
          )}

          <section className="indicador" aria-labelledby="ind-1">
            <header>
              <h2 id="ind-1">¿Dónde y cuándo fallamos más?</h2>
              <p className="tenue">
                Entrega en primer intento: {porcentaje(exitososPI, totalPI)} de {totalPI.toLocaleString('es-AR')} intentos. La celda muestra la tasa de fallo.
              </p>
            </header>
            <div className="mapa-calor" role="table" aria-label="Tasa de fallo en primer intento por zona y franja">
              <div role="row" className="mapa-fila mapa-cabecera">
                <span role="columnheader">Zona</span>
                {FRANJAS.map((f) => (
                  <span role="columnheader" key={f}>{ETIQUETA_FRANJA[f]}</span>
                ))}
              </div>
              {zonasVisibles.map((z) => (
                <div role="row" className="mapa-fila" key={z.id}>
                  <span role="rowheader">
                    {z.nombre} <span className="tenue">{nombreHub(z.hubId)}</span>
                  </span>
                  {FRANJAS.map((f) => {
                    const c = celdas.get(`${z.id}|${f}`);
                    const tasa = c?.total ? 1 - c.exitosos / c.total : null;
                    const destacada = peor && peor.zonaId === z.id && peor.franja === f;
                    return (
                      <span role="cell" key={f} className={`mapa-celda${destacada ? ' destacada' : ''}`} style={tonoFallo(tasa)}>
                        {tasa === null ? '—' : `${Math.round(tasa * 100)} %`}
                        <small>{c?.total ?? 0}</small>
                      </span>
                    );
                  })}
                </div>
              ))}
            </div>
            <div className="serie" aria-label="Entrega en primer intento por día">
              {d.porDia.map((p) => (
                <div key={p._id} className="serie-barra">
                  <span className="serie-relleno" style={{ height: `${p.total ? (100 * p.exitosos) / p.total : 0}%` }}>
                    <span className="serie-valor">{porcentaje(p.exitosos, p.total)}</span>
                  </span>
                  <span className="serie-etiqueta">{dia(p._id)}</span>
                </div>
              ))}
            </div>
          </section>

          <section className="indicador" aria-labelledby="ind-2">
            <header>
              <h2 id="ind-2">¿Por qué fallamos?</h2>
              <p className="tenue">{totalFallos.toLocaleString('es-AR')} entregas fallidas, todos los intentos.</p>
            </header>
            <ul className="leyenda">
              {MOTIVOS.map((m) => (
                <li key={m}>
                  <span className={`muestra motivo-${m.toLowerCase()}`} aria-hidden="true" />
                  {ETIQUETA_MOTIVO[m]} <strong>{porcentaje(totalMotivos[m], totalFallos)}</strong>
                </li>
              ))}
            </ul>
            <div className="apiladas">
              {zonasVisibles.map((z) => {
                const m = motivosPorZona.get(z.id) ?? {};
                const total = MOTIVOS.reduce((s, k) => s + (m[k] ?? 0), 0);
                return (
                  <div key={z.id} className="apilada">
                    <span className="apilada-nombre">{z.nombre}</span>
                    <span className="apilada-barra">
                      {MOTIVOS.map((k) =>
                        m[k] ? (
                          <span key={k} className={`motivo-${k.toLowerCase()}`} style={{ flexGrow: m[k] }} title={`${ETIQUETA_MOTIVO[k]}: ${m[k]}`} />
                        ) : null
                      )}
                    </span>
                    <span className="apilada-total cifra">{total}</span>
                  </div>
                );
              })}
            </div>
          </section>

          <section className="indicador" aria-labelledby="ind-3">
            <header>
              <h2 id="ind-3">¿Qué hub llega tarde?</h2>
              <p className="tenue">
                Entregas dentro de la promesa, por hub y día.
                {peorHub && ` El más rezagado es ${peorHub.nombre} (${porcentaje(peorHub.enPlazo, peorHub.conPromesa)}).`}
              </p>
            </header>
            <table className="tabla tabla-sla">
              <thead>
                <tr>
                  <th scope="col">Hub</th>
                  {fechasSla.map((f) => (
                    <th scope="col" key={f} className="cifra">{dia(f)}</th>
                  ))}
                  <th scope="col" className="cifra">Total</th>
                </tr>
              </thead>
              <tbody>
                {hubsVisibles.map((h) => {
                  const t = slaHub(h.id);
                  return (
                    <tr key={h.id}>
                      <th scope="row">{h.nombre}</th>
                      {fechasSla.map((f) => {
                        const s = sla.get(`${h.id}|${f}`);
                        const tasa = s?.conPromesa ? s.enPlazo / s.conPromesa : null;
                        return (
                          <td key={f} className={`cifra ${tasa !== null && tasa < 0.85 ? 'bajo' : ''}`}>
                            {s ? porcentaje(s.enPlazo, s.conPromesa) : '—'}
                          </td>
                        );
                      })}
                      <td className="cifra"><strong>{porcentaje(t.enPlazo, t.conPromesa)}</strong></td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </section>

          <section className="indicador" aria-labelledby="ind-4">
            <header>
              <h2 id="ind-4">¿Cuánto espera un paquete en el hub?</h2>
              <p className="tenue">Tiempo medio entre el ingreso al hub y la salida a reparto (primer intento).</p>
            </header>
            {d.tiempos.length === 0 ? (
              <div className="vacio-panel">
                <p>Este indicador todavía no está activo. Se activa con el sistema en marcha y completa el histórico leyendo Kafka desde el inicio:</p>
                <pre>docker compose --profile replay up -d analitica-tiempos</pre>
              </div>
            ) : (
              <div className="tiempos">
                {tiemposPorHub.map((t) => (
                  <div key={t.id} className="tiempo">
                    <span className="tiempo-nombre">{t.nombre}</span>
                    <span className="tiempo-barra">
                      <span style={{ width: `${((t.media ?? 0) / maxTiempo) * 100}%` }} />
                    </span>
                    <span className="cifra">
                      {t.media === null ? '—' : `${Math.floor(t.media / 60)} h ${Math.round(t.media % 60)} min`}
                      <small className="tenue"> ({t.n} envíos)</small>
                    </span>
                  </div>
                ))}
                <p className="tenue nota">
                  Días con datos: {[...new Set(d.tiempos.map((t) => t._id.fecha))].sort().map(dia).join(', ')}.
                </p>
              </div>
            )}
          </section>
        </>
      )}
    </div>
  );
}
