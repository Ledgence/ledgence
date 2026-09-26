export type Theme = "system" | "light" | "dark";
export const themeStorageKey = "ledgence.console.theme";
export function readTheme(): Theme {
  try {
    const value = localStorage.getItem(themeStorageKey);
    return value === "light" || value === "dark" ? value : "system";
  } catch {
    return "system";
  }
}
export function applyTheme(theme: Theme): void {
  if (theme === "system") delete document.documentElement.dataset.theme;
  else document.documentElement.dataset.theme = theme;
  try {
    localStorage.setItem(themeStorageKey, theme);
  } catch {
    /* A blocked preference store does not prevent operation. */
  }
}
