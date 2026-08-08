# Modelo matemático del DAG de Jaiba

## Propósito

Este documento formaliza el grafo de procesamiento ejecutado por Jaiba. La
notación sirve para razonar sobre validación, routing, concurrencia, colas y
backpressure; no sustituye el manifiesto YAML ni los tipos de Rust.

## 1. Grafo de flujo

Un flujo se representa como un grafo dirigido, etiquetado y con capacidades:

$$
G=(V,E,\tau,\rho,c)
$$

donde:

- \(V\) es el conjunto finito y no vacío de procesadores;
- \(E\subseteq V\times V\) es el conjunto de conexiones dirigidas;
- \(\tau:V\rightarrow T\) asigna un tipo de procesador a cada nodo;
- \(\rho:E\rightarrow R\) asigna una relación de routing a cada conexión;
- \(c:E\rightarrow\mathbb{N}^{+}\) asigna una capacidad de cola positiva.

El conjunto de relaciones soportadas por el diseñador actual es:

$$
R=\{\mathtt{success},\mathtt{failure},\mathtt{train},
\mathtt{validation},\mathtt{test}\}.
$$

En el núcleo, `relationship` se conserva como texto para permitir que los
procesadores y plugins añadan relaciones sin modificar la estructura del
grafo.

Cada identificador de procesador debe ser único:

$$
\forall u,v\in V,\quad id(u)=id(v)\Rightarrow u=v.
$$

Toda conexión debe referirse a procesadores existentes:

$$
\forall (u,v)\in E,\quad u\in V\land v\in V.
$$

## 2. Condición acíclica

Jaiba acepta el flujo solamente si no contiene ciclos dirigidos:

$$
\nexists\,v_1,\ldots,v_k\in V:\quad
v_1\rightarrow v_2\rightarrow\cdots\rightarrow v_k\rightarrow v_1.
$$

Esta condición equivale a la existencia de un orden topológico

$$
\pi:V\rightarrow\{1,\ldots,|V|\}
$$

que satisface:

$$
(u,v)\in E\Rightarrow\pi(u)<\pi(v).
$$

La implementación calcula \(\pi\) con el algoritmo de Kahn. Primero define el
grado de entrada de cada nodo:

$$
d^{-}(v)=|\{u\in V:(u,v)\in E\}|.
$$

Después procesa los nodos con \(d^{-}(v)=0\) y elimina conceptualmente sus
aristas. Si procesa menos de \(|V|\) nodos, existe un ciclo y el flujo es
rechazado.

## 3. Procesamiento y emisiones

Sea \(\mathcal{P}\) el conjunto de paquetes válidos. Un procesador puede
producir cero, una o varias emisiones, por lo que se modela como:

$$
f_v:\mathcal{P}\rightarrow\mathcal{M}(\mathcal{P}\times R),
$$

donde \(\mathcal{M}(X)\) es un multiconjunto finito de elementos de \(X\).
Una emisión tiene la forma:

$$
(p',r)\in f_v(p),
$$

con paquete resultante \(p'\) y relación \(r\). El caso elemental de éxito o
fallo puede escribirse como:

$$
f_v(p)=
\begin{cases}
\{(p',\mathtt{success})\}, & \text{si el procesamiento termina correctamente},\\
\{(p_e,\mathtt{failure})\}, & \text{si termina con un error encaminable}.
\end{cases}
$$

Los destinos compatibles con una emisión son:

$$
D(v,r)=\{w\in V:(v,w)\in E\land\rho(v,w)=r\}.
$$

El runtime crea una unidad de trabajo para cada conexión compatible. Por ello,
si \(|D(v,r)|>1\), existe un *fan-out*: la misma emisión se encamina a varios
destinos. Si \(D(v,r)=\varnothing\), el paquete alcanzó el final de esa ruta.

## 4. Colas y backpressure

Para cada conexión \(e\in E\), sea \(Q_e(t)\) su cola en el instante \(t\).
Debe cumplirse:

$$
0\leq |Q_e(t)|\leq c(e).
$$

Sea \(C_G\in\mathbb{N}^{+}\) la capacidad global de trabajo pendiente. La
restricción global se expresa como:

$$
\sum_{e\in E}|Q_e(t)|\leq C_G.
$$

Una emisión que excedería una de estas cotas no se descarta: queda bloqueada
temporalmente hasta que exista capacidad. De forma abstracta, una emisión por
la conexión \(e\) se admite cuando:

$$
|Q_e(t)|+1\leq c(e)
\quad\land\quad
\sum_{a\in E}|Q_a(t)|+1\leq C_G.
$$

Esta espera constituye el backpressure del motor.

## 5. Concurrencia por procesador

Para cada procesador \(v\), se definen:

- \(w_v\geq1\): tareas concurrentes configuradas;
- \(a_v(t)\): tareas activas;
- \(q_v(t)\): trabajos pendientes destinados al procesador;
- \(m_v\): máximo opcional de trabajos en vuelo.

El límite local es:

$$
0\leq a_v(t)\leq w_v.
$$

Cuando \(m_v\) está configurado, también se exige:

$$
a_v(t)+q_v(t)\leq m_v.
$$

Si \(W\) es el límite global de tareas activas, entonces:

$$
\sum_{v\in V}a_v(t)\leq W.
$$

Los límites controlan la ejecución simultánea, pero no cambian la condición
acíclica ni el orden parcial definido por el DAG.

## 6. Reintentos y garantía de entrega

Sea \(A_v\geq0\) el número máximo de reintentos de \(v\), \(d_{0,v}\) el
retardo inicial y \(d_{\max,v}\) el retardo máximo. El backoff exponencial del
reintento \(k\) puede describirse como:

$$
d_v(k)=\min\left(d_{\max,v},d_{0,v}2^k\right).
$$

Jaiba ofrece entrega *at-least-once*. Para un paquete persistido \(p\), el
número de ejecuciones observables cumple:

$$
N(p)\geq1,
$$

pero no se garantiza \(N(p)=1\) ante recuperación después de una falla. Por
eso, los destinos deben ser idempotentes o emplear claves únicas o `upsert`.

## 7. Ejemplo: `basic-flow.yaml`

El flujo básico contiene:

$$
V=\{source,transform,encode,destination\}
$$

y:

$$
E=\{(source,transform),(transform,encode),(encode,destination)\}.
$$

Todas las conexiones usan `success`:

$$
\forall e\in E,\quad\rho(e)=\mathtt{success}.
$$

Un orden topológico válido es:

$$
source\prec transform\prec encode\prec destination.
$$

Para una emisión única por procesador, el recorrido de datos puede resumirse
mediante la composición:

$$
F=f_{destination}\circ f_{encode}\circ f_{transform}\circ f_{source}.
$$

En este ejemplo, `source` genera registros, `transform` renombra campos,
`encode` serializa el contenido como JSON y `destination` registra el
resultado. La composición es una simplificación válida para esta cadena
lineal; un DAG con bifurcaciones se describe mejor mediante \(D(v,r)\) y sus
emisiones, no como una sola composición de funciones.

## 8. Correspondencia con el código

| Concepto matemático | Representación en Jaiba |
|---|---|
| \(V\), \(\tau\) | `GraphNode { id, processor_type }` |
| \(E\), \(\rho\) | `GraphEdge { from, relationship, to }` |
| \(c(e)\) | `ConnectionConfig.queue.capacity` |
| \(\pi\) | `FlowGraph.topological_order` |
| \(f_v\) | implementación de cada procesador y sus emisiones |
| \(D(v,r)\) | selección de conexiones por origen y relación |
| \(w_v,m_v\) | `SchedulingConfig.concurrent_tasks` y `maximum_in_flight` |
| \(C_G,W\) | límites globales del motor |

Las fuentes principales son `crates/jaiba-core/src/graph.rs`,
`crates/jaiba-core/src/config/flow.rs` y
`crates/jaiba-runtime/src/engine/executor.rs`.
