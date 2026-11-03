// SPDX-License-Identifier: MIT
import { useState, type ReactNode } from "react";
import { Link, useLocation } from "react-router";
import {
  Activity,
  ArrowUpRight,
  BookOpen,
  Inbox,
  ShieldCheck,
  FileText,
  Layers2,
  LogOut,
  ChevronDown,
  Monitor,
  Moon,
  Sun,
  Cpu,
  PanelLeftClose,
  Server,
} from "lucide-react";
import { useAccess, signOut } from "../app/access-session";
import { applyTheme, readTheme, type Theme } from "../app/theme";
import "../styles/shell.css";

const sidebarStorageKey = "ledgence.console.sidebar-collapsed";
const sections = [
  { to: "/executions", title: "Executions", icon: Activity },
  { to: "/programs", title: "Programs", icon: Layers2 },
  { to: "/approvals", title: "Approvals", icon: Inbox },
  { to: "/effects", title: "Effects", icon: ShieldCheck },
  { to: "/workers", title: "Workers", icon: Server },
];

function readSidebarCollapsed(): boolean {
  try {
    return localStorage.getItem(sidebarStorageKey) === "true";
  } catch {
    return false;
  }
}

function AccountControls({
  subject,
  issuer,
}: {
  subject: string;
  issuer: string;
}) {
  return (
    <div className="shell-account">
      <p className="shell-actor sidebar-label wrap" title={issuer}>
        Signed in as <strong>{subject}</strong>
      </p>
      <button
        type="button"
        onClick={() => {
          void signOut();
        }}
        aria-label="Sign out of this browser"
        title="Sign out of this browser"
        className="shell-signout"
      >
        <LogOut aria-hidden="true" />
        <span className="sidebar-label">Sign out</span>
      </button>
    </div>
  );
}

export function AppShell({
  instanceName,
  serverVersion,
  children,
}: {
  instanceName: string;
  serverVersion?: string | undefined;
  children: ReactNode;
}) {
  const { pathname } = useLocation();
  const access = useAccess();
  const identity =
    access.phase === "ready" && access.access?.mode === "credentials"
      ? access.access
      : null;
  const section = pathname.split("/")[1];
  const activeSection =
    section === "workflows"
      ? "executions"
      : section === "agents"
        ? "programs"
        : section;
  const [theme, setTheme] = useState<Theme>(readTheme);
  const [collapsed, setCollapsed] = useState(readSidebarCollapsed);
  const ThemeIcon = { system: Monitor, light: Sun, dark: Moon }[theme];
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
          <div className="shell-sidebar-header">
            {!collapsed && (
              <Link
                className="brand shell-brand"
                to="/executions"
                aria-label="Ledgence Console home"
              >
                <span className="brand-mark" aria-hidden="true" />
                <span className="brand-wordmark sidebar-label">
                  ledgence<span className="brand-product">Console</span>
                </span>
              </Link>
            )}
            {/* Keep this button mounted so keyboard focus survives either shape. */}
            <button
              className={`shell-collapse${collapsed ? " shell-collapse--brand" : ""}`}
              type="button"
              onClick={toggleSidebar}
              aria-expanded={!collapsed}
              aria-controls="console-navigation"
              aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
              title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
            >
              {collapsed ? (
                <span className="brand-mark" aria-hidden="true" />
              ) : (
                <PanelLeftClose aria-hidden="true" />
              )}
            </button>
          </div>
          <div className="shell-workspace-id">
            <span className="brand-mark" aria-hidden="true" />
            <span title={instanceName}>{instanceName}</span>
          </div>
          <nav
            id="console-navigation"
            className="navigation shell-navigation"
            aria-label="Main navigation"
          >
            {sections.map(({ to, title, icon: Icon }) => (
              <Link
                key={to}
                to={to}
                aria-label={title}
                title={collapsed ? title : undefined}
                aria-current={to === `/${activeSection}` ? "page" : undefined}
              >
                <Icon aria-hidden="true" />
                <span className="sidebar-label">{title}</span>
              </Link>
            ))}
          </nav>
          <div className="sidebar-footer shell-sidebar-footer">
            {identity && <AccountControls {...identity} />}

            <span className="shell-caption sidebar-label" aria-hidden="true">
              Resources
            </span>
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
            <label className="shell-theme" title={`Appearance: ${theme}`}>
              <ThemeIcon aria-hidden="true" />
              <span className="shell-theme-label" aria-hidden="true">
                Appearance
              </span>
              <span className="shell-theme-value" aria-hidden="true">
                {theme}
              </span>
              <ChevronDown className="shell-theme-chevron" aria-hidden="true" />
              <select
                aria-label="Appearance"
                value={theme}
                onChange={(event) => changeTheme(event.target.value)}
              >
                <option value="system">System</option>
                <option value="light">Light</option>
                <option value="dark">Dark</option>
              </select>
            </label>
            {serverVersion && (
              <div
                className="shell-version"
                title={`Server version ${serverVersion}`}
              >
                <Cpu aria-hidden="true" />
                <span className="sidebar-label">Server version</span>
                <code className="sidebar-label">{serverVersion}</code>
              </div>
            )}
          </div>
        </aside>
        <div className="workspace console-workspace">
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
