"""Genera THIRD_PARTY_LICENSES.md: licencias de todo lo que va dentro de los binarios.

Cubre los crates del motor (todos los conectores + consola incrustada, Linux y
Windows), los crates del escritorio Tauri, los paquetes npm de producción de la
consola y las bibliotecas de C que se compilan dentro del binario.

Requisitos: cargo-about (versión fija en .github/workflows/ci.yml) y
`npm ci` en apps/jaiba-ui (los textos npm salen de node_modules).

Uso:
  python3 scripts/third-party-licenses.py          # reescribe el archivo
  python3 scripts/third-party-licenses.py --check  # falla si está desactualizado
"""

import json
from pathlib import Path
import re
import subprocess
import sys
import tomllib


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "THIRD_PARTY_LICENSES.md"
ABOUT_CONFIG = ROOT / "about.toml"
UI_DIR = ROOT / "apps" / "jaiba-ui"
DESKTOP_MANIFEST = UI_DIR / "src-tauri" / "Cargo.toml"
ENGINE_FEATURES = "jaiba-cli/all-drivers jaiba-cli/embedded-ui"

# Código nativo que se compila o enlaza estáticamente dentro de los binarios y
# cuya licencia no es la del crate de Rust que lo envuelve. Las rutas son
# relativas al crate; "repo:" indica un texto guardado en este repositorio.
NATIVE_LIBRARIES = [
    ("rdkafka-sys", "librdkafka", ["librdkafka/LICENSES.txt"]),
    ("curl-sys", "curl", ["curl/COPYING"]),
    ("libz-sys", "zlib", ["src/zlib/LICENSE"]),
    ("aws-lc-sys", "AWS-LC", ["aws-lc/LICENSE"]),
    ("odpic-sys", "ODPI-C", ["odpi/LICENSE.txt"]),
    (
        "webview2-com-sys",
        "Microsoft WebView2 SDK (WebView2LoaderStatic.lib, solo Windows)",
        ["repo:licenses/webview2-sdk-LICENSE.txt"],
    ),
]

HEADER = """\
# Licencias de terceros

Jaiba se distribuye bajo la licencia Apache-2.0 (ver [`LICENSE`](LICENSE)). Los
binarios publicados (`jaiba` para Linux y Windows, la imagen Docker y el
instalador de escritorio) incluyen software de terceros. Cada componente
conserva su propia licencia, cuyo texto se reproduce abajo.

Este archivo se genera con `python3 scripts/third-party-licenses.py`; no lo
edites a mano. El CI falla si queda desactualizado respecto a `Cargo.lock`,
`apps/jaiba-ui/src-tauri/Cargo.lock` o `apps/jaiba-ui/package-lock.json`.

## Notas

- **Oracle Instant Client**: no se redistribuye. `jaiba oracle-client install`
  lo descarga de oracle.com y su uso queda sujeto a la licencia de Oracle.
- **OpenSSL** (solo Linux): el binario usa la `libssl` del sistema; no se incluye.
- **SQLite** va incluido y es de dominio público
  (<https://sqlite.org/copyright.html>); no tiene texto de licencia que reproducir.
- Las bibliotecas del sistema que usa el escritorio en Linux (GTK, WebKitGTK)
  se enlazan dinámicamente y no se incluyen.
"""


def run(command, cwd=ROOT):
    return subprocess.run(command, cwd=cwd, check=True, capture_output=True, text=True).stdout


def accepted_licenses():
    accepted = tomllib.loads(ABOUT_CONFIG.read_text())["accepted"]
    allowed = tomllib.loads((ROOT / "deny.toml").read_text())["licenses"]["allow"]
    if sorted(accepted) != sorted(allowed):
        sys.exit("`accepted` de about.toml y `allow` de deny.toml deben tener las mismas licencias")
    return accepted


def cargo_about(extra):
    manifest = [arg for arg in extra if arg.startswith("--manifest-path")]
    run(["cargo", "fetch", "--locked", "--quiet", *manifest])
    output = run(
        ["cargo", "about", "generate", "-c", str(ABOUT_CONFIG), "--format", "json", "--offline", "--locked", *extra]
    )
    return json.loads(output)


def normalize(text):
    lines = [line.rstrip() for line in text.replace("\r\n", "\n").replace("\r", "\n").strip("\n").split("\n")]
    return "\n".join(lines)


def add_group(groups, license, text, users):
    """Agrupa textos que solo difieren en espacios; se queda con uno estable."""
    text = normalize(text)
    group = groups.setdefault((license, " ".join(text.split())), {"text": text, "users": set()})
    group["text"] = min(group["text"], text)
    group["users"].update(users)


def notice_files(directory):
    return sorted(path for path in directory.iterdir() if path.is_file() and path.name.upper().startswith("NOTICE"))


def rust_licenses(reports):
    """Grupos de licencia, crate -> (versión, directorio) y avisos NOTICE."""
    groups = {}
    crate_dirs = {}
    notices = {}
    for report in reports:
        for crate in report["crates"]:
            package = crate["package"]
            if not package.get("source"):
                continue
            directory = Path(package["manifest_path"]).parent
            crate_dirs[package["name"]] = (package["version"], directory)
            for path in notice_files(directory):
                notices[(package["name"], package["version"])] = normalize(path.read_text(errors="replace"))
        for license in report["licenses"]:
            users = {
                (used["crate"]["name"], used["crate"]["version"])
                for used in license["used_by"]
                if used["crate"].get("source")
            }
            if users:
                add_group(groups, license["id"], license["text"], users)
    return groups, crate_dirs, notices


def choose_license(expression, accepted):
    expression = expression.strip().strip("()")
    if " AND " in expression:
        return [choose_license(part, accepted)[0] for part in expression.split(" AND ")]
    options = [part.strip().strip("()") for part in expression.split(" OR ")]
    for license in accepted:
        if license in options:
            return [license]
    sys.exit(f"npm: la licencia «{expression}» no está en about.toml (accepted)")


def license_files(directory):
    names = ("license", "licence", "copying")
    return sorted(path for path in directory.iterdir() if path.is_file() and path.name.lower().startswith(names))


def npm_licenses(accepted, notices):
    lock = json.loads((UI_DIR / "package-lock.json").read_text())
    groups = {}
    for path, package in sorted(lock["packages"].items()):
        if not path or package.get("dev") or package.get("devOptional"):
            continue
        name = path.split("node_modules/")[-1]
        directory = UI_DIR / path
        if not directory.is_dir():
            sys.exit(f"npm: falta {path}; corre `npm ci` en apps/jaiba-ui")
        files = license_files(directory)
        if not files:
            sys.exit(f"npm: {name} no trae archivo de licencia")
        user = (name, package["version"])
        for license in choose_license(package.get("license", ""), accepted):
            token = license.split("-")[0].upper()
            chosen = [path for path in files if token in path.name.upper()] or files
            text = "\n\n".join(normalize(path.read_text(errors="replace")) for path in chosen)
            add_group(groups, license, text, {user})
        for path in notice_files(directory):
            notices[user] = normalize(path.read_text(errors="replace"))
    return groups


def native_sections(crate_dirs):
    sections = []
    for crate, component, files in NATIVE_LIBRARIES:
        if crate not in crate_dirs:
            sys.exit(f"{crate} ya no está en el árbol de dependencias; quítalo de NATIVE_LIBRARIES")
        version, directory = crate_dirs[crate]
        texts = []
        for file in files:
            path = ROOT / file.removeprefix("repo:") if file.startswith("repo:") else directory / file
            if not path.is_file():
                sys.exit(f"{crate} {version}: no existe {file}; revisa NATIVE_LIBRARIES")
            texts.append(normalize(path.read_text(errors="replace")))
        sections.append((component, f"{crate} {version}", "\n\n".join(texts)))
    return sections


def fenced(text):
    longest = max((len(run) for run in re.findall(r"`+", text)), default=0)
    fence = "`" * max(3, longest + 1)
    return f"{fence}text\n{text}\n{fence}\n"


def render(rust, npm, native, notices, accepted):
    order = {license: index for index, license in enumerate(accepted)}
    counts = {}
    for groups, column in ((rust, 0), (npm, 1)):
        seen = {}
        for (license, _), group in groups.items():
            seen.setdefault(license, set()).update(group["users"])
        for license, users in seen.items():
            counts.setdefault(license, [0, 0])[column] = len(users)

    mpl = sorted({user for (license, _), group in rust.items() if license.startswith("MPL") for user in group["users"]})
    mpl_note = ""
    if mpl:
        links = ", ".join(f"[`{name} {version}`](https://crates.io/crates/{name}/{version})" for name, version in mpl)
        mpl_note = f"- Crates bajo MPL-2.0, usados sin modificar; su código fuente está en crates.io: {links}.\n"
    out = [HEADER, mpl_note, "\n## Resumen\n\n| Licencia | Crates de Rust | Paquetes npm |\n| --- | ---: | ---: |\n"]
    for license in sorted(counts, key=lambda license: (order.get(license, len(order)), license)):
        crates, packages = counts[license]
        out.append(f"| {license} | {crates} | {packages} |\n")
    out.append(
        "\nUn crate puede contar en más de una fila cuando su licencia combina varias "
        "(por ejemplo `Apache-2.0 AND ISC`).\n"
    )

    out.append("\n## Bibliotecas nativas incluidas en el binario\n")
    for component, crate, text in native:
        out.append(f"\n### {component}\n\nIncluida a través de `{crate}`.\n\n{fenced(text)}")

    out.append("\n## Avisos (archivos NOTICE)\n")
    for (name, version), text in sorted(notices.items()):
        out.append(f"\n### {name} {version}\n\n{fenced(text)}")

    def groups_section(title, groups):
        out.append(f"\n## {title}\n")
        ordered = sorted(
            groups.items(),
            key=lambda item: (order.get(item[0][0], len(order)), -len(item[1]["users"]), min(item[1]["users"])),
        )
        for (license, _), group in ordered:
            names = ", ".join(f"`{name} {version}`" for name, version in sorted(group["users"]))
            out.append(f"\n### {license}\n\nUsada por: {names}\n\n{fenced(group['text'])}")

    groups_section("Crates de Rust (motor y escritorio)", rust)
    groups_section("Paquetes npm (consola web y escritorio)", npm)
    return "".join(out)


def main():
    check = "--check" in sys.argv[1:]
    accepted = accepted_licenses()
    engine = cargo_about(["--workspace", "--features", ENGINE_FEATURES])
    desktop = cargo_about([f"--manifest-path={DESKTOP_MANIFEST}"])
    rust, crate_dirs, notices = rust_licenses([engine, desktop])
    npm = npm_licenses(accepted, notices)
    content = render(rust, npm, native_sections(crate_dirs), notices, accepted)

    if check:
        current = OUTPUT.read_text() if OUTPUT.exists() else ""
        if current != content:
            sys.exit(
                "THIRD_PARTY_LICENSES.md está desactualizado: corre "
                "`python3 scripts/third-party-licenses.py` y sube el resultado."
            )
        print("THIRD_PARTY_LICENSES.md al día")
        return
    OUTPUT.write_text(content)
    print(f"escrito {OUTPUT.relative_to(ROOT)} ({len(content) // 1024} KiB)")


if __name__ == "__main__":
    main()
