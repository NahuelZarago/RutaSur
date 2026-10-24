export const metadata = { title: 'Seguí tu envío · RutaSur' };

export default function LayoutPublico({ children }) {
  return (
    <div className="publico">
      <header className="publico-barra">
        <a href="/tracking" className="marca">
          <span className="marca-señal" aria-hidden="true" />
          RutaSur
        </a>
      </header>
      <main className="publico-contenido">{children}</main>
    </div>
  );
}
