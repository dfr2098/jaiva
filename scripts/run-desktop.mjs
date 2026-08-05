import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const uiDirectory = path.join(root, "apps", "jaiba-ui");
const action = process.argv[2];
const environment = { ...process.env };

if (process.platform === "linux") {
  environment.GDK_BACKEND ??= "x11";
  environment.WEBKIT_DISABLE_DMABUF_RENDERER ??= "1";
}

let command = process.execPath;
let args;
if (action === "dev" || action === "build") {
  const tauriCli = path.join(uiDirectory, "node_modules", "@tauri-apps", "cli", "tauri.js");
  if (!existsSync(tauriCli)) {
    throw new Error("No se encontró Tauri CLI. Ejecuta `npm ci` en apps/jaiba-ui.");
  }
  args = [tauriCli, action];
} else if (action === "run") {
  const suffix = process.platform === "win32" ? ".exe" : "";
  command = path.join(uiDirectory, "src-tauri", "target", "debug", `jaiba-desktop${suffix}`);
  args = [];
  if (!existsSync(command)) {
    throw new Error(`No existe ${command}. Ejecuta primero \`npm run desktop:dev\`.`);
  }
} else {
  throw new Error("Acción inválida. Usa dev, build o run.");
}

const result = spawnSync(command, args, {
  cwd: uiDirectory,
  env: environment,
  stdio: "inherit",
});

if (result.error) {
  throw result.error;
}
process.exit(result.status ?? 1);
