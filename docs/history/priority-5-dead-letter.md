# Prioridad 5: dead-letter y logs operativos

## Paquetes fallidos

Cuando un procesador agota sus reintentos, el elemento persistente pasa a
`DEAD_LETTER`. Se conservan el contenido, atributos, esquema, identificadores,
procesador, número final de intento, fecha y mensaje de error.

Consultar hasta 100 elementos:

```bash
cargo run -- dead-letter list flow.yaml 100
```

Preparar un elemento para reproceso:

```bash
cargo run -- dead-letter replay flow.yaml QUEUE_ID
cargo run -- flow.yaml
```

El primer comando cambia únicamente `DEAD_LETTER` a `PENDING`, reinicia los
intentos y registra `REQUEUED` en procedencia. La siguiente ejecución del flujo
lo recupera desde el repositorio. Un elemento no se reprocesa automáticamente,
para evitar ciclos de fallo sin supervisión.

## Logs persistentes

Los logs se emiten simultáneamente a consola y a la carpeta configurada. La
escritura de archivo usa un worker no bloqueante y conserva un guard hasta el
cierre para vaciar el buffer.

```yaml
engine:
  logging:
    enabled: true
    directory: /var/log/jaiva
    rotation: hourly
    retention_hours: 168
    cleanup_interval_seconds: 900
```

Este ejemplo rota cada hora, conserva siete días y revisa archivos vencidos
cada quince minutos. Jaiva nunca elimina nombres ajenos a su propio prefijo.

Para producción, la carpeta debe pertenecer al usuario del servicio y disponer
de espacio y permisos apropiados. `never` evita rotación, pero la retención no
puede eliminar un archivo activo cuya fecha de modificación continúa
actualizándose.
