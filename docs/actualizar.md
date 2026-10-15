# Notas de actualización

Cambios que pueden obligar a revisar flows existentes al subir de versión. Las
mejoras sin efecto en flows existentes quedan en las notas del release.

## v0.8 → v0.9

### MySQL / MariaDB

`query_mysql` cambia el formato de algunos tipos y los conectores MySQL tienen
ahora límite de espera y no reintentan rechazos:

- `DATETIME`/`TIMESTAMP` salen con `T` en lugar de espacio
  (`"2026-10-13T10:20:30"`).
- `DECIMAL` que no cabe exacto en un número sale como **texto** con todos los
  dígitos (antes `f64` redondeado). `FLOAT` sale con su valor corto (`0.1`).
- `TIME` sin ceros de relleno en la fracción y con signo / más de 24 h.
- `GEOMETRY` siempre en base64.
- 300 s de espera máxima por fila leída (`call_timeout_ms`, `0` = sin límite) y
  por sentencia escrita.
- Clave duplicada, NULL, dato muy largo, tabla inexistente y sintaxis van a
  `failure` sin reintentar.

Tabla completa, ejemplos y cómo conservar el comportamiento anterior:
[mysql.md § Cambios respecto a v0.8](mysql.md#cambios-respecto-a-v08-revisa-tus-flows).
