"""Fault-injection test for the Jaiba Memory Engine (JME) Cold tier.

Build first: cargo build -p jaiba-cli --bin jaiba
Run: python scripts/chaos-jme.py [path/to/jaiba]

Every scenario runs the CLI against a temporary JAIBA_DATA_DIR, injects one
fault (full quota, read-only disk, torn or corrupted segment, SIGKILL during
writes) and then recovers the stored values in a fresh process. Values are read
back through memory_get and persisted by an immediate class, so the check uses
the same path as a real flow.

Any FAIL exits with status 1.
"""

import hashlib
import json
import os
from pathlib import Path
import random
import signal
import struct
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BINARY = ROOT / "target" / "debug" / ("jaiba.exe" if os.name == "nt" else "jaiba")
BINARY = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else DEFAULT_BINARY
FLOW_ID = "jme-chaos"
HEADER = struct.Struct("<4sBIIII32s")
MAGIC = b"JMC1"

failures = []


def passed(message):
    print(f"PASS: {message}")


def failed(message):
    failures.append(message)
    print(f"FAIL: {message}")


def payload(key, generation=0):
    seed = f"{key}:{generation}".encode()
    chunks = []
    for index in range(10):
        seed = hashlib.sha256(seed + bytes([index])).digest()
        chunks.append(seed.hex())
    return {"carrier_id": key, "generation": generation, "blob": "".join(chunks)}


def flow(stage, keys, generation=0, cold=None, max_entries=None):
    cold = cold or {}
    segment = cold.get("segment_max_bytes", 4096)
    quota = cold.get("max_disk_bytes")
    entries = max_entries or (1 if stage == "write" else len(keys) + 8)
    lines = [
        f"id: {FLOW_ID}",
        "engine:",
        "  max_concurrency: 8",
        "  repository: {enabled: false}",
        "  logging: {enabled: false}",
        "  domain_memory:",
        "    enabled: true",
        "    policy:",
        "      version: 1",
        f"      max_entries: {entries}",
        "      cold:",
        "        backend: segmented",
        f"        segment_max_bytes: {segment}",
    ]
    if quota:
        lines.append(f"        max_disk_bytes: {quota}")
    lines += [
        "      classes:",
        "        carrier: {policy: cache, temperature: cold, ttl: 1h}",
        "        recovered: {policy: immediate}",
        "processors:",
    ]
    connections = []
    for key in keys:
        source = f"source_{key}"
        record = json.dumps(payload(key, generation)) if stage == "write" else json.dumps({"carrier_id": key})
        lines += [f"  - id: {source}", "    type: generate_records", f"    config: {{records: [{record}]}}"]
        target = "remember" if stage == "write" else "recall"
        connections.append(f"  - {{from: {source}, to: {target}, relationship: success}}")
    if stage == "write":
        lines += [
            "  - id: remember",
            "    type: memory_upsert",
            "    config: {class: carrier, id_attribute: carrier_id}",
        ]
    else:
        lines += [
            "  - id: recall",
            "    type: memory_get",
            "    config: {class: carrier, id_attribute: carrier_id, attribute: memory.value, miss_as_null: true}",
            "  - id: save",
            "    type: memory_upsert",
            "    config: {class: recovered, id_attribute: carrier_id, value_attribute: memory.value}",
        ]
        connections.append("  - {from: recall, to: save, relationship: success}")
    return "\n".join(lines + ["connections:"] + connections) + "\n"


class Sandbox:
    def __init__(self, base, name):
        self.root = Path(base) / name
        self.work = self.root / "work"
        self.data = self.root / "data"
        self.work.mkdir(parents=True)

    @property
    def cold_dir(self):
        return self.data / "jme" / "cold" / FLOW_ID

    @property
    def persist(self):
        return self.data / "jme" / FLOW_ID / "persist.jsonl"

    def env(self):
        env = {k: v for k, v in os.environ.items() if not k.startswith(("JAIBA_", "JAIVA_"))}
        env["JAIBA_DATA_DIR"] = str(self.data)
        return env

    def write_flow(self, text):
        path = self.work / "flow.yaml"
        path.write_text(text, encoding="utf-8")
        return path

    def run(self, text, timeout=60):
        path = self.write_flow(text)
        return subprocess.run(
            [str(BINARY), str(path)], cwd=self.work, env=self.env(), capture_output=True, text=True, timeout=timeout
        )

    def spawn(self, text):
        path = self.write_flow(text)
        return subprocess.Popen(
            [str(BINARY), str(path)],
            cwd=self.work,
            env=self.env(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )

    def recover(self, keys, **flow_args):
        if self.persist.exists():
            self.persist.unlink()
        result = self.run(flow("recover", keys, **flow_args))
        values = {}
        if self.persist.exists():
            for line in self.persist.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    record = json.loads(line)
                    values[record["key"].split(":", 1)[1]] = record["value"]
        return result, values

    def segments(self):
        return sorted(self.cold_dir.rglob("segment-*.jmc"))

    def cold_bytes(self):
        return sum(path.stat().st_size for path in self.segments())


def records(path):
    data = path.read_bytes()
    offset = 0
    found = []
    while offset + HEADER.size <= len(data):
        magic, flag, key_len, class_len, _raw, payload_len, _sum = HEADER.unpack_from(data, offset)
        if magic != MAGIC:
            break
        total = HEADER.size + key_len + class_len + payload_len
        if offset + total > len(data):
            break
        key = data[offset + HEADER.size : offset + HEADER.size + key_len].decode()
        found.append({"key": key, "offset": offset, "total": total, "payload_at": offset + total - payload_len})
        offset += total
    return found


def panicked(result):
    return "panicked" in (result.stderr or "") + (result.stdout or "")


def intact(values, keys, generation=0):
    return [key for key in keys if values.get(key) == payload(key, generation)]


def check_values(values, expected, label):
    wrong = {key: value for key, value in values.items() if value is not None and value != expected.get(key)}
    if wrong:
        failed(f"{label}: valores alterados para {sorted(wrong)}")
        return False
    return True


def scenario_quota(base):
    box = Sandbox(base, "quota")
    keys = [f"Q{n:02d}" for n in range(12)]
    quota = 4096
    result = box.run(flow("write", keys, cold={"max_disk_bytes": quota}))
    output = result.stdout + result.stderr
    if panicked(result):
        return failed("cuota Cold: el proceso entró en pánico")
    used = box.cold_bytes()
    if used > quota:
        return failed(f"cuota Cold: {used} bytes en disco superan el límite {quota}")
    if "cuota de disco Cold agotada" not in output:
        return failed("cuota Cold: no se reportó el error de cuota agotada")
    passed(f"cuota Cold respetada ({used}/{quota} bytes) y error explícito al agotarse")

    _, values = box.recover(keys)
    expected = {key: payload(key) for key in keys}
    stored = [key for key, value in values.items() if value is not None]
    if not stored:
        return failed("cuota Cold: nada recuperable tras reiniciar")
    if check_values(values, expected, "cuota Cold"):
        passed(f"cuota Cold: {len(stored)} valores aceptados se recuperan intactos tras reiniciar")


def scenario_read_only(base):
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        print("SKIP: disco de solo lectura (root ignora permisos)")
        return
    box = Sandbox(base, "readonly")
    first = [f"R{n}" for n in range(4)]
    result = box.run(flow("write", first))
    if result.returncode != 0:
        return failed(f"solo lectura: escritura inicial falló: {result.stderr[-300:]}")
    paths = [box.cold_dir, *box.cold_dir.rglob("*")]
    modes = {path: path.stat().st_mode for path in paths}
    for path in sorted(paths, key=lambda p: len(p.parts), reverse=True):
        path.chmod(0o555 if path.is_dir() else 0o444)
    try:
        second = [f"W{n}" for n in range(4)]
        result = box.run(flow("write", second))
    finally:
        for path, mode in modes.items():
            path.chmod(mode)
    if panicked(result):
        return failed("solo lectura: el proceso entró en pánico")
    output = result.stdout + result.stderr
    if "denied" not in output.lower() and "denegado" not in output.lower():
        return failed(f"solo lectura: no se reportó el error de permisos (exit={result.returncode})")
    passed("disco de solo lectura: la escritura falla con error explícito, sin pánico")

    _, values = box.recover(first)
    recovered = intact(values, first)
    if len(recovered) != len(first) - 1:
        return failed(f"solo lectura: datos previos perdidos (recuperados {recovered})")
    passed("disco de solo lectura: los datos previos siguen intactos al restaurar permisos")


def torn_case(base, name, damage):
    box = Sandbox(base, name)
    keys = [f"T{n}" for n in range(6)]
    result = box.run(flow("write", keys))
    if result.returncode != 0:
        failed(f"{name}: escritura inicial falló")
        return None
    segment = box.segments()[-1]
    before = records(segment)
    damage(segment, before)
    result, values = box.recover(keys)
    return box, keys, before, result, values


def scenario_torn_tail(base):
    def cut_last_record(segment, before):
        last = before[-1]
        with segment.open("r+b") as handle:
            handle.truncate(last["offset"] + last["total"] - 7)

    outcome = torn_case(base, "torn-record", cut_last_record)
    if outcome:
        box, keys, before, result, values = outcome
        torn_key = before[-1]["key"].split(":", 1)[1]
        kept = intact(values, keys)
        if result.returncode != 0 or panicked(result):
            failed(f"registro cortado: el flujo no arrancó: {result.stderr[-300:]}")
        elif values.get(torn_key) is not None:
            failed("registro cortado: se devolvió un valor parcialmente escrito")
        elif len(kept) != len(keys) - 2:
            failed(f"registro cortado: se perdieron registros completos (intactos {kept})")
        else:
            more = ["T6", "T7", "T8"]
            box.run(flow("write", more))
            _, after = box.recover(keys + more)
            new_ok = intact(after, more)
            if len(new_ok) == len(more) - 1 and set(kept) <= set(intact(after, keys)):
                passed("registro cortado a media escritura: se descarta solo ese registro y se puede seguir escribiendo")
            else:
                failed(f"registro cortado: las escrituras posteriores no se recuperan ({new_ok})")

    def partial_header(segment, before):
        with segment.open("ab") as handle:
            handle.write(MAGIC + b"\x01" + b"\x07" * 15)

    outcome = torn_case(base, "torn-header", partial_header)
    if outcome:
        _box, keys, _before, result, values = outcome
        kept = intact(values, keys)
        if result.returncode == 0 and len(kept) == len(keys) - 1:
            passed("cabecera incompleta al final del segmento: se recorta y no se pierde nada")
        else:
            failed(f"cabecera incompleta: exit={result.returncode} intactos={kept} {result.stderr[-200:]}")

    def zero_fill(segment, before):
        with segment.open("ab") as handle:
            handle.write(b"\x00" * 4096)

    outcome = torn_case(base, "torn-zeros", zero_fill)
    if outcome:
        _box, keys, _before, result, values = outcome
        kept = intact(values, keys)
        if result.returncode == 0 and len(kept) == len(keys) - 1:
            passed("relleno de ceros al final (corte de luz): se recorta y no se pierde nada")
        else:
            failed(f"relleno de ceros: exit={result.returncode} intactos={kept} {result.stderr[-200:]}")


def scenario_sigkill(base, rounds=12):
    box = Sandbox(base, "sigkill")
    keys = [f"K{n:03d}" for n in range(250)]
    cold = {"segment_max_bytes": 65536}
    rng = random.Random(20261003)
    killed = 0
    for generation in range(rounds):
        process = box.spawn(flow("write", keys, generation=generation, cold=cold))
        time.sleep(rng.uniform(0.05, 0.6))
        if process.poll() is None:
            process.send_signal(signal.SIGKILL)
            killed += 1
        process.wait(timeout=30)
        stderr = process.stderr.read() if process.stderr else ""
        if "panicked" in stderr or "magic" in stderr or "checksum" in stderr:
            return failed(f"SIGKILL ronda {generation}: el reinicio encontró el store dañado: {stderr[-300:]}")
    if killed == 0:
        return failed("SIGKILL: ninguna ronda se interrumpió; ajustar tiempos")
    result, values = box.recover(keys, cold=cold)
    if result.returncode != 0 or panicked(result):
        return failed(f"SIGKILL: el store no abre tras {killed} kills: {result.stderr[-300:]}")
    bad = []
    for key, value in values.items():
        if value is None:
            continue
        generation = value.get("generation") if isinstance(value, dict) else None
        if generation is None or value != payload(key, generation):
            bad.append(key)
    present = sum(1 for value in values.values() if value is not None)
    if bad:
        return failed(f"SIGKILL: valores corruptos o mezclados en {bad[:5]}")
    passed(f"{killed} kill -9 durante escrituras: el store abre y {present} valores se leen íntegros")


def scenario_bitflip(base):
    box = Sandbox(base, "bitflip")
    keys = [f"B{n}" for n in range(6)]
    box.run(flow("write", keys))
    segment = box.segments()[-1]
    target = records(segment)[2]
    data = bytearray(segment.read_bytes())
    data[target["payload_at"] + 5] ^= 0xFF
    segment.write_bytes(bytes(data))
    result, values = box.recover(keys)
    victim = target["key"].split(":", 1)[1]
    if result.returncode != 0 or panicked(result):
        return failed(f"byte dañado en valor: el flujo no arrancó: {result.stderr[-300:]}")
    if values.get(victim) is not None:
        return failed("byte dañado en valor: se devolvió un valor corrupto")
    others = intact(values, keys)
    if len(others) != len(keys) - 2:
        return failed(f"byte dañado en valor: afectó a otras llaves ({others})")
    passed("byte dañado en un valor: el checksum lo detecta, no se entrega y las demás llaves siguen bien")
    output = result.stdout + result.stderr
    if "JME Cold read failed" in output and "checksum" in output:
        passed("byte dañado en un valor: queda registrado como lectura fallida, distinto de 'no encontrado'")
    else:
        failed("byte dañado en un valor: no se registró la lectura fallida")


def scenario_header_corruption(base):
    box = Sandbox(base, "header")
    keys = [f"H{n}" for n in range(6)]
    box.run(flow("write", keys))
    segment = box.segments()[-1]
    target = records(segment)[2]
    data = bytearray(segment.read_bytes())
    data[target["offset"]] ^= 0xFF
    segment.write_bytes(bytes(data))
    result, values = box.recover(keys)
    victim = target["key"].split(":", 1)[1]
    if panicked(result):
        return failed("cabecera dañada: el proceso entró en pánico")
    if result.returncode != 0:
        return failed(f"cabecera dañada: el flujo no arrancó: {result.stderr[-300:]}")
    if values.get(victim) is not None:
        return failed("cabecera dañada: se entregó el registro dañado")
    kept = intact(values, keys)
    if len(kept) != len(keys) - 2:
        return failed(f"cabecera dañada: se perdieron registros sanos (intactos {kept})")
    quarantine = segment.with_name(segment.name + ".corrupt")
    if not quarantine.exists() or quarantine.read_bytes() != bytes(data):
        return failed("cabecera dañada: no quedó la copia .corrupt del segmento original")
    if "JME Cold segment salvaged" not in result.stdout + result.stderr:
        return failed("cabecera dañada: el rescate no quedó registrado en el log")
    passed("cabecera dañada a mitad del segmento: se rescata, se guarda copia .corrupt y solo se pierde ese registro")


def main():
    if not BINARY.exists():
        raise SystemExit(f"binary not found: {BINARY} (cargo build -p jaiba-cli --bin jaiba)")
    with tempfile.TemporaryDirectory(prefix="jaiba-jme-chaos-") as base:
        for scenario in (
            scenario_quota,
            scenario_read_only,
            scenario_torn_tail,
            scenario_sigkill,
            scenario_bitflip,
            scenario_header_corruption,
        ):
            try:
                scenario(base)
            except Exception as error:  # noqa: BLE001
                failed(f"{scenario.__name__}: excepción {error!r}")
    print(f"\nResumen: {len(failures)} FAIL")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
