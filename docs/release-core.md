# Release-core y Prioridad 1 (defaults seguros)

Objetivo: hacer Jaiba **más difícil de usar mal** antes de añadir features.

## Defaults seguros (código)

| Situación | Comportamiento |
| --- | --- |
| `serve` en loopback sin `JAIBA_MASTER_KEY` | Almacén en memoria + warning (dev) |
| `JAIBA_REQUIRE_MASTER_KEY=1` sin clave | **Falla** al arrancar |
| Bind no-loopback (`0.0.0.0`, LAN) sin clave | **Falla**, salvo `JAIBA_ALLOW_INMEMORY_SECRETS=1` |
| `authentication: none` fuera de loopback | **Falla** (ya existía) |
| Bearer sin token / users file | **Falla** (ya existía) |

Variables:

```bash
export JAIBA_MASTER_KEY='passphrase-larga'
export JAIBA_REQUIRE_MASTER_KEY=1          # lab / staging
# solo emergencia en red:
# export JAIBA_ALLOW_INMEMORY_SECRETS=1
export JAIBA_ADMIN_TOKEN='…'              # o JAIBA_ADMIN_USERS_FILE
```

## Feature `release-core`

En `jaiba-cli`:

```bash
cargo build -p jaiba-cli --features release-core --bin jaiba
```

Es el perfil **sin** `oracle-driver` / `kafka-driver` / `mongodb-driver` /
`sqlserver-driver`. Postgres/SQLite siguen en el runtime base.

## Smoke local / CI

```bash
./scripts/smoke-release-core.sh
```

Stack Estable (Postgres + serve + UI) y smoke CSV:

```bash
./scripts/release-core-up.sh
./scripts/smoke-stable-path.sh
```

Ver también [product-roadmap.md](product-roadmap.md). Recorrido Estable
Postgres→CSV: workflow **Stable path** (automático en `main` + cron laborable).

## Congelar roadmap

**Vigente:** no abrir fases `priority-11+` de producto hasta que **smoke +
release-core + Stable path** lleven **2 semanas seguidas en verde**.

Señales que deben estar verdes de forma continua:

| Señal | Cómo |
| --- | --- |
| `smoke-release-core` | job CI en cada push/PR (`scripts/smoke-release-core.sh`) |
| Stable path (Postgres→CSV) | push a `main`/`master`, cron lun–vie, label `stable-path`, o `workflow_dispatch` |

Reglas mientras dure el freeze:

- No crear `docs/history/priority-11*.md` ni nuevas fases numeradas de producto.
- Los `docs/history/priority-*.md` existentes quedan como historial.
- El trabajo permitido es: defaults seguros, tests/regresión, docs de onboarding,
  y los ciclos 2–4 del [product-roadmap.md](product-roadmap.md) (mantenibilidad /
  producción / extensibilidad) **sin** abrir una fase `priority-11+`.
- Un rojo en CI o en la regresión Compose reinicia el contador de 2 semanas.

Ver también: [guia-para-nuevos.md](guia-para-nuevos.md), [ci.md](ci.md).
