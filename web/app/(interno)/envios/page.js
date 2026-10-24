import Link from 'next/link';
import { listarEnvios } from '@/lib/consultas';
import { ESTADOS_DESVIO, ESTADOS_RECORRIDO, ETIQUETA_ESTADO, diaHora, nombreZona, zonas } from '@/lib/formato';

export const dynamic = 'force-dynamic';

export default async function Envios({ searchParams }) {
  const { estado = '', zona = '', q = '' } = searchParams;
  const envios = await listarEnvios({ estado, zona, q });

  return (
    <div className="pagina">
      <h1>Envíos</h1>
      <form className="filtros" method="get">
        <label>
          Código
          <input name="q" defaultValue={q} placeholder="RS-…" />
        </label>
        <label>
          Estado
          <select name="estado" defaultValue={estado}>
            <option value="">Todos</option>
            {[...ESTADOS_RECORRIDO, ...ESTADOS_DESVIO].map((e) => (
              <option key={e} value={e}>
                {ETIQUETA_ESTADO[e]}
              </option>
            ))}
          </select>
        </label>
        <label>
          Zona
          <select name="zona" defaultValue={zona}>
            <option value="">Todas</option>
            {zonas.map((z) => (
              <option key={z.id} value={z.id}>
                {z.nombre}
              </option>
            ))}
          </select>
        </label>
        <button className="boton" type="submit">
          Filtrar
        </button>
      </form>

      {envios.length === 0 ? (
        <p className="vacio-panel">No hay envíos con esos filtros. Probá con otro estado o dispará un escenario desde el panel de demo.</p>
      ) : (
        <table className="tabla">
          <thead>
            <tr>
              <th scope="col">Envío</th>
              <th scope="col">Estado</th>
              <th scope="col">Zona</th>
              <th scope="col">Último evento</th>
              <th scope="col">Promesa</th>
              <th scope="col" className="cifra">Intentos</th>
            </tr>
          </thead>
          <tbody>
            {envios.map((e) => (
              <tr key={e._id}>
                <td>
                  <Link href={`/envios/${e._id}`} className="codigo">
                    {e._id}
                  </Link>
                </td>
                <td>
                  <span className={`etiqueta etiqueta-${(e.estadoVisible ?? '').toLowerCase()}`}>{ETIQUETA_ESTADO[e.estadoVisible]}</span>
                </td>
                <td>{nombreZona(e.zonaId)}</td>
                <td className="cifra">{diaHora(e.ultimoEvento?.ocurridoEn)}</td>
                <td className="cifra">{diaHora(e.promesa?.venceEn)}</td>
                <td className="cifra">{e.intentos}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <p className="tenue nota">Se muestran los 100 envíos con actividad más reciente.</p>
    </div>
  );
}
