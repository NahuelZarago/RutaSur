// Catálogo maestro del caso: hubs, zonas, franjas y comercios.
// La web tiene una copia en web/lib/catalogo.json (un test verifica que sean idénticas).
import { readFileSync } from 'node:fs';

export const catalogo = JSON.parse(
  readFileSync(new URL('./catalogo.json', import.meta.url), 'utf8')
);

const zonasPorId = new Map(catalogo.zonas.map((z) => [z.id, z]));
const hubsPorId = new Map(catalogo.hubs.map((h) => [h.id, h]));

export function zonaPorId(id) {
  const zona = zonasPorId.get(id);
  if (!zona) throw new Error(`Zona desconocida: ${id}`);
  return zona;
}

export function hubPorId(id) {
  return hubsPorId.get(id) ?? null;
}

/** Repartidor de una zona: R-001 … R-117 (13 por zona, 9 zonas). */
export function repartidorDeZona(zonaId, indice) {
  const posicion = catalogo.zonas.findIndex((z) => z.id === zonaId);
  const n = posicion * catalogo.repartidoresPorZona + (indice % catalogo.repartidoresPorZona) + 1;
  return `R-${String(n).padStart(3, '0')}`;
}
