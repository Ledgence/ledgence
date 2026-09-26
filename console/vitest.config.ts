import { defineConfig } from "vitest/config";
import { playwright } from "@vitest/browser-playwright";
import react from "@vitejs/plugin-react";
export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: "unit",
          environment: "node",
          include: ["tests/unit/**/*.test.ts"],
        },
      },
      {
        plugins: [react()],
        resolve: { dedupe: ["react", "react-dom"] },
        optimizeDeps: {
          include: [
            "react",
            "react-dom/client",
            "react/jsx-runtime",
            "react-router",
            "@tanstack/react-query",
            "lossless-json",
            "lucide-react",
            "@radix-ui/react-dialog",
            "vitest-browser-react",
          ],
        },
        test: {
          name: "components",
          include: ["tests/components/**/*.test.tsx"],
          browser: {
            enabled: true,
            headless: true,
            provider: playwright(),
            instances: [{ browser: "chromium" }, { browser: "webkit" }],
          },
        },
      },
    ],
  },
});
