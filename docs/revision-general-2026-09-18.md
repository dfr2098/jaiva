# Revisión general — 18 de septiembre de 2026

> Actualización posterior: los ocho hallazgos descritos abajo tienen cambios
> correctivos en el árbol de trabajo. Se conserva el diagnóstico original
> como referencia. La validación de las correcciones figura al final.
>
> Las referencias `archivo.rs:línea` apuntan al código de esa fecha. Desde el
> Ciclo 2, `engine/executor.rs` vive en `engine/executor/` y
> `connection_api.rs` en `connection_api/`.

Revisión del árbol de trabajo, incluidos los cambios locales de permisos y
memoria realizados en esta conversación. No se corrigieron hallazgos durante
esta revisión. Las reproducciones usaron archivos temporales y procesos de
prueba aislados.

## Resultado

El proyecto compila y la suite actual pasa, pero hay fallos funcionales que
requieren atención antes de considerar fiables los límites de memoria, el
despliegue con recuperación y la edición visual de flujos existentes.

### 1. Alta: espera circular cuando se agota la memoria de paquetes

**Reproducido.** Un flujo `generate_records → rename_fields → log_records`,
sin repositorio ni logging, termina con `JAIBA_MEMORY_MAX_BYTES=262144`.
Con `65536`, la misma ejecución no termina dentro de cinco segundos y el
proceso de prueba se cancela.

El ejecutor conserva la reserva de entrada hasta que termina el procesador
(`crates/jaiba-runtime/src/engine/executor.rs:513`). La transformación intenta
reservar memoria de salida antes de devolver el control
(`crates/jaiba-runtime/src/engine/processor.rs:85`). Si la entrada ocupa todo
el presupuesto, nadie puede liberar el permiso que necesita la salida. El
timeout de procesador es opcional y no está activado por defecto.

El nuevo presupuesto global conserva esta limitación y permite reproducirla
con un límite pequeño. Hace falta gestionar la transferencia de reservas o
una política de admisión que garantice progreso; aumentar el presupuesto no
resuelve el caso general. Añadir regresiones con transformaciones y varios
flujos simultáneos al límite.

### 2. Alta: Hot rechaza una escritura después de persistirla

**Reproducido.** Con clase `immediate`, `max_hot_bytes: 1`, un registro y
`retry.maximum_attempts: 1`, se observaron dos líneas en `persist.jsonl` y
un aviso de reintento por `HotByteCapacity`. La ejecución CLI terminó con
código 0 pese al fallo del procesador.

`MemoryManager::upsert_at` persiste antes de llamar a `hot_upsert`
(`crates/jaiba-memory/src/manager.rs:433`). El nuevo límite Hot puede rechazar
el valor después de ese efecto externo. Reintentar vuelve a persistirlo.
La prueba anterior de conservación de Hot solo cubría la política volátil;
faltó cubrir la interacción con persistencia inmediata.

Validar capacidad antes de persistir y definir el resultado de operaciones
parcialmente aplicadas. Probar inserción, actualización y reintentos con
`immediate` y `persistent`.

### 3. Alta: el diseñador elimina configuración al importar y exportar

**Reproducido ejecutando las funciones reales de TypeScript.** Un YAML con
`engine.state_file`, `engine.domain_memory` y
`engine.repository.database_path` pierde las tres opciones después de
`parseFlowYaml` y `toYaml`. Solo conserva `repository.enabled` en ese ejemplo.

El modelo importado y `engineBlock` representan un subconjunto del YAML
(`apps/jaiba-ui/src/builder/yaml.ts:85`, `:568`). El exportador reconstruye
el documento sin conservar las propiedades restantes. Guardar desde el
diseñador puede desactivar JME o cambiar ubicaciones de persistencia.

Conservar campos no editables o rechazar explícitamente la edición de una
configuración que no pueda preservarse. Añadir pruebas de importar/exportar
con ejemplos reales del repositorio.

### 4. Alta: el despliegue no detecta errores asíncronos de arranque

**Reproducido por HTTP.** Desplegar con `start=true` un flujo cuya política
JME apunta a un archivo inexistente devuelve HTTP 200 y estado `STARTING`.
Después, el registro mantiene la versión `DEPLOYED` y activa, mientras el
runtime está `FAILED`.

`FlowSupervisor::start` devuelve éxito tras crear la tarea, antes de ejecutar
la inicialización real (`crates/jaiba-runtime/src/engine/supervisor.rs:97`).
El registro solo restaura la versión anterior si esa llamada devuelve error
(`crates/jaiba-server/src/flow_registry.rs:455`). Por tanto, ese mecanismo
no cubre errores posteriores de conexiones, política JME o estado.

Separar la confirmación de inicialización de la ejecución del flujo y
esperarla antes de archivar la versión anterior. Probar el reemplazo de un
flujo sano por otro que falla al inicializar.

### 5. Alta: la conexión directa del escritorio carece de CORS

**Respuesta de API reproducida; aplicación gráfica no ejecutada.** Una
petición `OPTIONS /api/v1/flows` con origen `http://tauri.localhost`, método
solicitado `PUT` y headers `authorization,content-type` devuelve 405 sin
`Access-Control-Allow-Origin`.

La UI usa `fetch` directamente contra la base de API y Tauri selecciona una
URL de loopback (`apps/jaiba-ui/src/main.tsx`, `src/api.ts:140`). El router
del servidor no configura CORS
(`crates/jaiba-server/src/observability.rs:384`). La política CSP del
escritorio permite conexiones, pero no sustituye la autorización CORS de
la API. El proxy de Vite/nginx puede ocultar el problema en el modo web.

Definir los orígenes permitidos para escritorio y remoto, o usar un transporte
nativo controlado. Validar desde la aplicación empaquetada.

### 6. Alta: las ramas duplican paquetes sin reservar memoria adicional

**Confirmado por inspección, sin medición de RAM.** Al distribuir un paquete
a varias conexiones, se clona `DataPacket` para cada destino, pero se comparte
la misma reserva mediante `Arc`
(`crates/jaiba-runtime/src/engine/executor.rs:769`). El contenido usa
`Vec<Value>` o `Vec<u8>` y su clonación copia datos, no solo una referencia.
Además, cada intento de procesador recibe otra copia (`:589`).

Un paquete con muchos destinos puede consumir mucho más de lo contabilizado.
El presupuesto global limita las reservas actuales, no esas copias. Compartir
contenido inmutable o contabilizar copias con una estrategia que también
resuelva el progreso bajo presión del punto 1.

### 7. Media: checkpoints concurrentes comparten el mismo archivo temporal

**Confirmado por inspección; carrera no reproducida.** `StateStore::persist`
libera el mutex tras serializar y después escribe y renombra siempre el
mismo `.tmp` (`crates/jaiba-runtime/src/engine/state.rs:47`). Dos escritores
pueden competir por ese archivo, devolver errores o sobrescribir un snapshot
más nuevo con uno anterior. Instancias diferentes que usen el mismo
`state_file` tampoco comparten el mutex.

Serializar el ciclo completo de actualización/persistencia, usar temporales
únicos y establecer la propiedad del archivo por flujo o por almacén.
Probar escrituras concurrentes y reapertura del estado.

### 8. Media: observabilidad pública sigue exponiendo información de flujos

**Confirmado por inspección.** `/ready` devuelve el snapshot completo del
primer flujo, incluidos errores, sin autenticar. `/metrics` tampoco exige
autenticación (`crates/jaiba-server/src/observability.rs:445`, `:477`).
El filtrado reciente de `/runtime` y WebSocket no cubre estas rutas.

Mantener una respuesta pública mínima de disponibilidad y definir cómo se
protegen las métricas o en qué red se publican. Esto afecta a despliegues
que exponen la API a usuarios con acceso restringido por proyecto; no implica
que un endpoint público de salud deba requerir necesariamente un token.

## Validación realizada y límites

- `cargo test --workspace --all-targets --locked --offline`: 167 resultados
  aprobados en la suite. Algunas pruebas de bases reales retornan sin probar
  una conexión cuando faltan variables `JAIBA_TEST_*`; el total no acredita
  integración real con todos los motores.
- `npm run build`: TypeScript y bundle de producción aprobados.
- `cargo check --manifest-path apps/jaiba-ui/src-tauri/Cargo.toml --locked
  --offline`: aprobado en Windows, tras permitir extraer dependencias en la
  caché de Cargo. No equivale a ejecutar ni empaquetar la aplicación.
- Formato y Clippy habían pasado sobre estos cambios; `git diff --check`
  también pasó durante la revisión.
- Reproducciones independientes de límite de paquetes, persistencia inmediata,
  importación/exportación YAML, respuesta CORS y despliegue fallido.
- Se revisaron además configuración del núcleo, repositorio, conexiones y
  secretos, SDK de plugins, scheduler, scripts, Compose y workflows.
- No se ejecutaron servicios externos, E2E de navegador, pruebas prolongadas
  de carga, todos los drivers opcionales, empaquetado ni despliegue Docker.
  Tampoco se hizo una auditoría de vulnerabilidades de dependencias.

## Orden recomendado

1. Corregir persistencia rechazada y pérdida de configuración YAML.
2. Resolver progreso y contabilidad de memoria bajo presión.
3. Confirmar arranque antes de completar despliegues y restauraciones.
4. Validar el escritorio con transporte/CORS correcto.
5. Cubrir concurrencia de checkpoints y exposición de observabilidad.

Cada corrección debe incluir la reproducción correspondiente como regresión.
La revisión es transversal y priorizada; no certifica ausencia de otros fallos.

## Correcciones y validación posterior

1. La reserva de memoria de transformaciones y ramas devuelve un error de
   capacidad en vez de esperar manteniendo recursos necesarios para avanzar.
   Con 64 KiB la reproducción termina con error; con 256 KiB se completa.
2. Hot valida bytes y capacidad antes de persistir. Se prueba tanto
   `immediate` como `persistent`, incluyendo actualizaciones y reintentos.
3. El diseñador conserva el documento importado y aplica los cambios del
   modelo visual. Las pruebas cubren opciones desconocidas, registros vacíos,
   edición de valores, cambio de nombre y eliminación de nodos/conexiones.
4. El supervisor espera una confirmación de inicialización del ejecutor. Los
   fallos de arranque llegan al registro antes de archivar la versión anterior.
5. CORS permite orígenes de escritorio y orígenes adicionales explícitos.
6. Se reserva memoria independiente para las copias de ramas e intentos; si no
   cabe una distribución completa, no se publican ramas parcialmente.
7. Los checkpoints comparten el estado por ruta canónica dentro del proceso,
   serializan todo el ciclo de escritura y usan temporales únicos. Una prueba
   de ocho instancias concurrentes verifica la persistencia de las ocho claves.
8. `/ready` solo expone un booleano; `/metrics` autentica y filtra proyectos.
   El Compose configura el token de Prometheus sin abrir las métricas al público.

Validación: suite Rust y Clippy, build de UI, tres pruebas de conservación YAML
sin navegador y `python scripts/review-regression.py` con procesos aislados.
`docker compose ... config --quiet` valida la configuración. No se ejecutó un
despliegue Docker ni una sesión gráfica del escritorio con estos cambios.
Los checkpoints siguen siendo un almacén de un proceso y la memoria sigue
siendo estimada; no se promete un límite exacto de RSS.
