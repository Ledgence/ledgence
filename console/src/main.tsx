import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./app/app";
import { applyTheme, readTheme } from "./app/theme";
import "./styles/global.css";
applyTheme(readTheme());
const element = document.getElementById("root");
if (!element) throw new Error("Console root element is missing.");
createRoot(element).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
