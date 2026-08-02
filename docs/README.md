# Documentación de Jaiva

Índice de la documentación del repositorio. Preferir estos documentos frente a
notas sueltas en el código.

## Empezar

| Documento | Contenido |
|---|---|
| [../README.md](../README.md) | Visión rápida, ejecutar, capacidades |
| [architecture.md](architecture.md) | Componentes y flujo de datos |
| [project-vision.md](project-vision.md) | Alcance y límites del producto |
| [configuration.md](configuration.md) | YAML de flujos, conexiones y features |
| [processors.md](processors.md) | Catálogo de procesadores |
| [operations.md](operations.md) | Servir, features, observabilidad, UI |

## Conexiones y bases de datos

| Documento | Contenido |
|---|---|
| [connection-manager.md](connection-manager.md) | UI/API de perfiles, MongoDB (campos y URL), SQL Server, seguridad |
| [priority-4-database-writes.md](priority-4-database-writes.md) | Escrituras multi-base |
| [oracle-to-postgres.md](oracle-to-postgres.md) | Ejemplo Oracle → PostgreSQL |

## Kafka, control y paralelismo

| Documento | Contenido |
|---|---|
| [priority-4-3-kafka.md](priority-4-3-kafka.md) | Publicación / consumo Kafka |
| [priority-5-dead-letter.md](priority-5-dead-letter.md) | Dead-letter y requeue |
| [priority-6-provenance.md](priority-6-provenance.md) | Provenance |
| [priority-7-control-plane.md](priority-7-control-plane.md) | API admin y endurecimiento |
| [priority-7-8-parallel-workers.md](priority-7-8-parallel-workers.md) | Workers por procesador |
| [priority-9-metrics.md](priority-9-metrics.md) | Métricas Prometheus |

## Pruebas de integración (Fase 8 de producto)

| Documento | Contenido |
|---|---|
| [priority-8-integration-tests.md](priority-8-integration-tests.md) | Harness contra Postgres, Kafka, MongoDB y SQL Server |
| [priority-8-visual-console.md](priority-8-visual-console.md) | Consola visual (otra numeración histórica; cubierta por `apps/jaiba-ui`) |

Script: [`../scripts/phase8-integration.sh`](../scripts/phase8-integration.sh)

## Modularización y bitácora

| Documento | Contenido |
|---|---|
| [modular-roadmap.md](modular-roadmap.md) | Fases 9.x del Connection Manager / plugins |
| [implementation-notes.md](implementation-notes.md) | Bitácora técnica, validaciones reales, limitaciones |

## Features de compilación habituales

```bash
# Connection Manager + procesadores Mongo
cargo run --features mongodb-driver -- serve examples/visualisa-flow.yaml

# SQL Server
cargo run --features sqlserver-driver -- serve examples/visualisa-flow.yaml

# Kafka publish/consume
cargo run --features kafka-driver -- serve examples/visualisa-flow.yaml

# Varios
cargo run --features kafka-driver,mongodb-driver,sqlserver-driver,oracle-driver \
  -- serve examples/visualisa-flow.yaml
```

Sin el feature, el tipo no aparece en `/api/v1/connection-types` ni en la UI.
