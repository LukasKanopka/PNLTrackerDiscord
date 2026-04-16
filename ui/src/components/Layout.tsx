import { Link, Outlet, useLocation } from 'react-router-dom';
import './layout.css';

export function Layout() {
  const loc = useLocation();
  return (
    <div className="app">
      <header className="topbar">
        <div className="topbar-inner">
          <div className="brand">
            <Link to="/" className="brand-link">
              PnL Tracker
            </Link>
            <span className="brand-sub">Discord logs → calls → P&L</span>
          </div>
          <nav className="nav">
            <Link className={loc.pathname === '/' ? 'nav-link active' : 'nav-link'} to="/">
              Runs
            </Link>
          </nav>
        </div>
      </header>
      <main className="main">
        <Outlet />
      </main>
      <footer className="footer">
        <span>Local UI · FastAPI + Postgres</span>
      </footer>
    </div>
  );
}

