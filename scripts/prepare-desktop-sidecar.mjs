import { execFileSync } from "node:child_process";
import { chmodSync, copyFileSync, existsSync, mkdirSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const argument = process.argv[2] || "debug";
const executableSuffix = process.platform === "win32" ? ".exe" : "";
const rustcOutput = execFileSync("rustc", ["-vV"], { encoding: "utf8" });
const tripleMatch = rustcOutput.match(/^host:\s+(.+)$/m);
const triple = tripleMatch ? tripleMatch[1].trim() : "";

if (!triple) {
  throw new Error("No se pudo determinar el target triple mediante `rustc -vV`");
}

let source;
if (argument === "debug" || argument === "release") {
  source = path.join(root, "target", argument, `jaiba${executableSuffix}`);
} else {
  source = path.resolve(argument);
}

if (!existsSync(source)) {
  throw new Error(
    `No existe ${source}\nCompila primero: cargo build -p jaiba-cli --bin jaiba${argument === "release" ? " --release" : ""}`,
  );
}

const destinationDirectory = path.join(root, "apps", "jaiba-ui", "src-tauri", "binaries");
const destination = path.join(destinationDirectory, `jaiba-${triple}${executableSuffix}`);
mkdirSync(destinationDirectory, { recursive: true });
copyFileSync(source, destination);
if (process.platform !== "win32") {
  chmodSync(destination, 0o755);
}

console.log(`Sidecar listo: ${destination}`);
