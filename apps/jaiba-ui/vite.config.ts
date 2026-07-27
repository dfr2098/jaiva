import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "127.0.0.1",
    port: 5173,
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
    sourcemap: true,
  },
});
