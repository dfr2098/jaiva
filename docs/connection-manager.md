# Administrador de conexiones

Jaiba UI incluye un módulo independiente para crear y comprobar perfiles de
conexión reutilizables. Se abre desde **Conexiones** en la barra principal.

## Separación de responsabilidades

- La UI captura la configuración y consume la API administrativa.
- `jaiba-server` valida los datos y nunca devuelve la contraseña.
- `jaiba-connection-manager` conserva perfiles, estado y referencias a secretos.
- Los plugins de conexión realizan la prueba específica del motor.
- Los flujos deben referirse al alias (`connection: postgres_dma`), no a una URL.

El proveedor predeterminado actual es `InMemorySecretStore`, pensado para
desarrollo local. Los perfiles y secretos desaparecen al reiniciar el motor. En
producción debe reemplazarse por Vault, Kubernetes Secrets o un almacén cifrado
con una clave externa; no se debe persistir el mapa en memoria como JSON.

## Drivers visibles

| Motor | Perfil | Prueba real |
| --- | --- | --- |
| PostgreSQL | Sí | Sí, SQLx |
| MySQL | Sí | Sí, SQLx |
| MariaDB | Sí | Sí, SQLx |
| Oracle | Se muestra si se compila el feature | Pendiente en API administrativa |
| SQL Server | Se muestra si se compila el feature | Pendiente en API administrativa |
| Kafka | Se muestra si se compila el feature | Se administrará como bus |
| OPC-UA / REST | Catálogo futuro | No |

La interfaz deshabilita motores que todavía no pueden probarse para evitar
crear configuraciones engañosas.

## API

- `GET /api/v1/connection-types`
- `GET /api/v1/connections`
- `POST /api/v1/connections`
- `GET|PUT|DELETE /api/v1/connections/{id}`
- `POST /api/v1/connections/{id}/duplicate`
- `POST /api/v1/connections/{id}/test`

Estas rutas usan la misma autenticación administrativa que los flujos. Una
prueba correcta informa disponibilidad, latencia, versión, uso del pool y fecha.
Una prueba fallida conserva la fecha y el diagnóstico sin incluir el secreto.

## Seguridad

- Las respuestas nunca contienen `password` ni `secret_ref`.
- La contraseña queda vacía al editar; dejarla vacía conserva la anterior.
- El frontend no usa `localStorage` ni `sessionStorage` para credenciales.
- Los logs no imprimen `ConnectionSecret`; su implementación de `Debug` redacta
  usuario y contraseña.
- Sin autenticación, el plano administrativo solo puede exponerse en loopback.
