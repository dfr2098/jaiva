# Documentación de Jaiba

## Si eres nuevo

**Única puerta:** **[guia-para-nuevos.md](guia-para-nuevos.md)**
(checklist de 5 líneas + `cargo run -- serve examples/basic-flow.yaml`).

No leas el resto todavía.

---

## Quiero X → feature Y

| Quiero… | Feature / comando |
| --- | --- |
| Arrancar en local | `cargo run -- serve examples/basic-flow.yaml` |
| Oracle | `--features oracle-driver` |
| MongoDB | `--features mongodb-driver` |
| Kafka | `--features kafka-driver` |
| SQL Server | `--features sqlserver-driver` |
| Smoke offline (sin serve) | `cargo run -- examples/smoke.yaml` |
| Postgres → CSV (Docker) | `./scripts/release-core-up.sh` |

```bash
cargo run --features mongodb-driver -- serve examples/basic-flow.yaml
cargo run --features kafka-driver,mongodb-driver,sqlserver-driver,oracle-driver \
  -- serve examples/basic-flow.yaml
```

---

## Por tarea (cuando ya corriste el arranque)

| Quiero… | Documento |
| --- | --- |
| Entender el producto | [project-vision.md](project-vision.md), [architecture.md](architecture.md) |
| Escribir YAML | [configuration.md](configuration.md) |
| Ver nodos | [processors.md](processors.md) |
| Perfiles / secretos | [connection-manager.md](connection-manager.md) |
| Operar UI / métricas | [operations.md](operations.md) |
| Madurez y ciclos | [product-roadmap.md](product-roadmap.md) |
| Defaults seguros / freeze | [release-core.md](release-core.md) |
| Empaque / `jaiba-serve` / WS | [packaging.md](packaging.md) |
| CI | [ci.md](ci.md) |

### Temas concretos

| Quiero… | Documento |
| --- | --- |
| Oracle → PostgreSQL | [oracle-to-postgres.md](oracle-to-postgres.md) |
| Memoria JME (capa fría) | [jme-cold-memory.md](jme-cold-memory.md) |
| AI Prep | [ai-data-prep.md](ai-data-prep.md) |
| Windows / WSL | [windows-native-and-wsl.md](windows-native-and-wsl.md) |
| Bitácora de decisiones | [implementation-notes.md](implementation-notes.md) |
| Plugins / Connection Manager | [modular-roadmap.md](modular-roadmap.md) |

### Historial (`priority-*`)

Movido a **[history/](history/)**. Son notas de diseño antiguas; no el onboarding.
