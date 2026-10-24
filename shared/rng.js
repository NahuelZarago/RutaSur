// Generador pseudoaleatorio con semilla (mulberry32). Con la misma semilla produce exactamente
// la misma secuencia, lo que permite repetir una corrida del simulador.

export function hashTexto(texto) {
  let h = 2166136261 >>> 0;
  for (let i = 0; i < texto.length; i++) {
    h ^= texto.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

export function crearRng(semilla) {
  let a = (typeof semilla === 'number' ? semilla : hashTexto(String(semilla))) >>> 0;

  const siguiente = () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };

  return {
    siguiente,
    entre: (min, max) => min + (max - min) * siguiente(),
    entero: (min, max) => Math.floor(min + (max - min + 1) * siguiente()),
    prob: (p) => siguiente() < p,
    elegir: (lista) => lista[Math.floor(siguiente() * lista.length)],
    /** pesos: { clave: peso } → clave elegida con probabilidad proporcional al peso. */
    ponderado: (pesos) => {
      const entradas = Object.entries(pesos);
      const total = entradas.reduce((s, [, w]) => s + w, 0);
      let r = siguiente() * total;
      for (const [clave, peso] of entradas) {
        r -= peso;
        if (r < 0) return clave;
      }
      return entradas[entradas.length - 1][0];
    },
  };
}
