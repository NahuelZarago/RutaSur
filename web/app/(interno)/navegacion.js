'use client';
import Link from 'next/link';
import { usePathname } from 'next/navigation';

const ENLACES = [
  { href: '/', texto: 'Torre de control' },
  { href: '/envios', texto: 'Envíos' },
  { href: '/analitica', texto: 'Analítica' },
  { href: '/demo', texto: 'Panel de demo' },
  { href: '/tracking', texto: 'Tracking público' },
];

export default function Navegacion() {
  const ruta = usePathname();
  const activo = (href) => (href === '/' ? ruta === '/' : ruta.startsWith(href));
  return (
    <nav className="nav" aria-label="Secciones">
      {ENLACES.map((e) => (
        <Link key={e.href} href={e.href} aria-current={activo(e.href) ? 'page' : undefined}>
          {e.texto}
        </Link>
      ))}
    </nav>
  );
}
