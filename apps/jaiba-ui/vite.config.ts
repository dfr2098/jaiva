import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const host = process.env.TAURI_DEV_HOST;

export default defineConfig({
  plugins: [react()],
  // Evita que Vite oculte errores de rustc durante `tauri dev`.
  clearScreen: false,
  envPrefix: ["VITE_", "TAURI_ENV_"],
  server: {
    host: host || "127.0.0.1",
    port: 5173,
    strictPort: true,
    hmr: host
      ? {
          protocol: "ws",
          host,
          port: 1421,
        }
      : undefined,
    watch: {
      ignored: ["**/src-tauri/**"],
    },
    proxy: {
      "/jaiva-api": {
        target: "http://127.0.0.1:9090",
        changeOrigin: true,
        ws: true,
        rewrite: (path) => path.replace(/^\/jaiva-api/, ""),
      },
      "/jaiba-api": {
        target: "http://127.0.0.1:9090",
        changeOrigin: true,
        ws: true,
        rewrite: (path) => path.replace(/^\/jaiba-api/, ""),
      },
    },
  },
  build: {
    outDir: "dist",
    sourcemap: !!process.env.TAURI_ENV_DEBUG,
    minify: process.env.TAURI_ENV_DEBUG ? false : "esbuild",
    // Vite 8 usa Safari 16.4 en su baseline y esbuild ya no transpila
    // destructuring para WebKit anterior. Windows conserva WebView2 105.
    target: process.env.TAURI_ENV_PLATFORM === "windows" ? "chrome105" : "safari16.4",
  },
});
