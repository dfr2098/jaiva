#!/usr/bin/env python3
"""Plugin de ejemplo para Jaiba: pasa a mayúsculas los campos indicados.

Protocolo v1 (JSON Lines): Jaiba escribe una petición JSON por línea en stdin
y espera exactamente una respuesta JSON por línea en stdout. Los logs van a
stderr. Solo usa la biblioteca estándar; cópialo como plantilla.
"""

import json
import sys

PROTOCOL_VERSION = 1


def reply(request_id, result=None, error=None):
    message = {
        "protocol_version": PROTOCOL_VERSION,
        "request_id": request_id,
        "result": result,
        "error": error,
    }
    print(json.dumps(message, ensure_ascii=False), flush=True)


def describe():
    return {
        "kind": "descriptor",
        "descriptor": {
            "id": "mayusculas",
            "version": "0.1.0",
            "display_name": "Mayúsculas",
            "category": "transform",
            "default_port": 0,
            "capabilities": ["records"],
        },
    }


def execute(configuration, packet):
    # Un paquete es {"Records": [...]} o {"Bytes": {"content": [...], "media_type": "..."}}.
    if "Records" not in packet:
        raise ValueError("solo acepta registros JSON")
    fields = configuration.get("fields", [])
    records = packet["Records"]
    for record in records:
        for field in fields:
            if isinstance(record.get(field), str):
                record[field] = record[field].upper()
    # Se pueden devolver cero, uno o varios paquetes.
    return {"kind": "packets", "packets": [{"Records": records}]}


def main():
    for line in sys.stdin:
        request = json.loads(line)
        request_id = request["request_id"]
        operation = request["operation"]
        try:
            if operation == "describe":
                reply(request_id, describe())
            elif operation == "execute":
                reply(request_id, execute(request["configuration"], request["packet"]))
            elif operation == "shutdown":
                reply(request_id, {"kind": "shutdown"})
                return
        except Exception as error:  # noqa: BLE001 - el error se informa a Jaiba
            print(f"mayusculas: {error}", file=sys.stderr)
            reply(
                request_id,
                error={"code": "invalid_input", "message": str(error), "retryable": False},
            )


if __name__ == "__main__":
    main()
