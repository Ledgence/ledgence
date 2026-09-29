// SPDX-License-Identifier: MIT
import { useState, type ReactNode } from "react";
import { NavLink, Link, useLocation } from "react-router";
import {
  Activity,
  ArrowUpRight,
  BookOpen,
  FileText,
  Layers2,
  Monitor,
  PanelLeftClose,
  PanelLeftOpen,
  Server,
} from "lucide-react";
import { applyTheme, readTheme, type Theme } from "../app/theme";
import "../styles/shell.css";

const sidebarStorageKey = "ledgence.console.sidebar-collapsed";
const sections = [
  { to: "/executions", title: "Executions", icon: Activity },
  { to: "/programs", title: "Programs", icon: Layers2 },
  { to: "/workers", title: "Workers", icon: Server },
];

function readSidebarCollapsed(): boolean {
  try {
    return localStorage.getItem(sidebarStorageKey) === "true";
  } catch {
    return false;
  }
}

export function AppShell({
  instanceName,
  children,
}: {
  instanceName: string;
  children: ReactNode;
}) {
  const location = useLocation();
  const [theme, setTheme] = useState<Theme>(readTheme);
  const [collapsed, setCollapsed] = useState(readSidebarCollapsed);
  function changeTheme(value: string) {
    if (value !== "system" && value !== "light" && value !== "dark") return;
    setTheme(value);
    applyTheme(value);
  }
  function toggleSidebar() {
    const next = !collapsed;
    setCollapsed(next);
    try {
      localStorage.setItem(sidebarStorageKey, String(next));
    } catch {
      // Navigation remains usable when preference storage is unavailable.
    }
  }
  return (
    <>
      <a className="skip-link" href="#main-content">
        Skip to content
      </a>
      <div
        className="console-shell console-shell--adaptive"
        data-sidebar-collapsed={collapsed}
      >
        <aside className="sidebar console-sidebar" aria-label="Console sidebar">
          <Link
            className="brand shell-brand"
            to="/executions"
            aria-label="Ledgence Console home"
            title={collapsed ? "Ledgence Console home" : undefined}
          >
            <span className="brand-mark" aria-hidden="true" />
            <span className="sidebar-label">ledgence</span>
          </Link>
          <nav
            id="console-navigation"
            className="navigation shell-navigation"
            aria-label="Main navigation"
          >
            {sections.map(({ to, title, icon: Icon }) => (
              <NavLink
                key={to}
                to={to}
                aria-label={title}
                title={collapsed ? title : undefined}
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
                <span className="sidebar-label">{title}</span>
              </NavLink>
            ))}
          </nav>
          <div className="sidebar-footer shell-sidebar-footer">
            <a
              href="https://docs.ledgence.com/"
              target="_blank"
              rel="noreferrer"
              aria-label="Documentation"
              title={collapsed ? "Documentation" : undefined}
            >
              <BookOpen aria-hidden="true" />
              <span className="sidebar-label">Documentation</span>
              <ArrowUpRight className="sidebar-label" aria-hidden="true" />
            </a>
            <a
              href="/console/notices/"
              aria-label="Notices"
              title={collapsed ? "Notices" : undefined}
            >
              <FileText aria-hidden="true" />
              <span className="sidebar-label">Notices</span>
            </a>
          </div>
        </aside>
        <div className="workspace console-workspace">
          <header className="instance-header shell-header">
            <div className="shell-header-leading">
              <button
                className="shell-collapse"
                type="button"
                onClick={toggleSidebar}
                aria-expanded={!collapsed}
                aria-controls="console-navigation"
                aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
                title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
              >
                {collapsed ? (
                  <PanelLeftOpen aria-hidden="true" />
                ) : (
                  <PanelLeftClose aria-hidden="true" />
                )}
              </button>
              <div className="instance-header-name">
                <span
                  className="brand-mark shell-mobile-mark"
                  aria-hidden="true"
                />
                <Server className="shell-instance-icon" aria-hidden="true" />
                <span title={instanceName}>{instanceName}</span>
              </div>
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
              <a
                href="https://docs.ledgence.com/"
                target="_blank"
                rel="noreferrer"
              >
                Documentation
              </a>
              <a href="/console/notices/">Notices</a>
            </footer>
          </main>
        </div>
      </div>
    </>
  );
}
