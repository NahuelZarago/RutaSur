import Navegacion from './navegacion';

export default function LayoutInterno({ children }) {
  return (
    <>
      <header className="barra">
        <a href="/" className="marca" aria-label="RutaSur, torre de control">
          <span className="marca-señal" aria-hidden="true" />
          RutaSur
        </a>
        <Navegacion />
      </header>
      <main className="contenido">{children}</main>
    </>
  );
}
