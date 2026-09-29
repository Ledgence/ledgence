import { useState, type ReactNode } from "react";
import { NavLink, Link, useLocation } from "react-router";
import {
  Activity,
  ArrowUpRight,
  BookOpen,
  Box,
  Layers2,
  Monitor,
  Server,
} from "lucide-react";
import { applyTheme, readTheme, type Theme } from "../app/theme";
const sections = [
  { to: "/executions", title: "Executions", icon: Activity },
  { to: "/programs", title: "Programs", icon: Layers2 },
];
export function AppShell({
  instanceName,
  children,
}: {
  instanceName: string;
  children: ReactNode;
}) {
  const location = useLocation();
  const [theme, setTheme] = useState<Theme>(readTheme);
  function changeTheme(value: string) {
    if (value !== "system" && value !== "light" && value !== "dark") return;
    setTheme(value);
    applyTheme(value);
  }
  return (
    <>
      <a className="skip-link" href="#main-content">
        Skip to content
      </a>
      <div className="console-shell">
        <aside className="sidebar">
          <div className="brand-wrap">
            <Link
              className="brand"
              to="/executions"
              aria-label="Ledgence Console home"
            >
              <span className="brand-mark" aria-hidden="true" />
              ledgence
            </Link>
            <span className="brand-label">Console</span>
          </div>
          <div className="instance-summary">
            <Server aria-hidden="true" />
            <div>
              <div className="instance-name">{instanceName}</div>
              <small>Self-hosted</small>
            </div>
          </div>
          <nav className="navigation" aria-label="Main navigation">
            {sections.map(({ to, title, icon: Icon }) => (
              <NavLink
                key={to}
                to={to}
                className={({ isActive }) =>
                  isActive ||
                  (to === "/executions" &&
                    location.pathname.startsWith("/workflows")) ||
                  (to === "/programs" &&
                    location.pathname.startsWith("/agents"))
                    ? "active"
                    : undefined
                }
              >
                <Icon aria-hidden="true" />
                <span>{title}</span>
              </NavLink>
            ))}
          </nav>
          <div className="navigation-label">Operations</div>
          <nav className="navigation" aria-label="Operations">
            <NavLink to="/workers">
              <Server aria-hidden="true" />
              <span>Workers</span>
            </NavLink>
          </nav>
          <div className="sidebar-footer">
            <a
              href="https://docs.ledgence.com/"
              target="_blank"
              rel="noreferrer"
            >
              <BookOpen aria-hidden="true" />
              Documentation
              <ArrowUpRight aria-hidden="true" />
            </a>
            <a href="/console/notices/">Notices</a>
            <small>Open source. Your infrastructure.</small>
          </div>
        </aside>
        <div className="workspace">
          <header className="instance-header">
            <div className="instance-header-name">
              <Box aria-hidden="true" />
              <span>{instanceName}</span>
            </div>
            <label className="theme-control">
              <Monitor aria-hidden="true" />
              <span className="sr-only">Appearance</span>
              <select
                value={theme}
                onChange={(event) => changeTheme(event.target.value)}
              >
                <option value="system">System</option>
                <option value="light">Light</option>
                <option value="dark">Dark</option>
              </select>
            </label>
          </header>
          <main id="main-content" className="main-content" tabIndex={-1}>
            {children}
            <footer className="mobile-notices">
              <a href="/console/notices/">Notices</a> ·{" "}
              <a
                href="https://docs.ledgence.com/"
                target="_blank"
                rel="noreferrer"
              >
                Documentation
              </a>
            </footer>
          </main>
        </div>
      </div>
    </>
  );
}
