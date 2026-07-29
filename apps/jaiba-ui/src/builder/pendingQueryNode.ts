// Handoff de un nodo Query desde el explorador de conexiones hacia el lienzo.
// El constructor visual deja aquí la consulta ya compilada; el FlowBuilder la
// consume al montarse y crea el nodo con su conexión asignada.

const PENDING_QUERY_NODE_KEY = "jaiba.pending.query.node";

export interface PendingQueryNode {
  connectionName: string;
  connectionType: string;
  query: string;
  parameters: unknown[];
  table: string;
}

export function stashPendingQueryNode(node: PendingQueryNode): void {
  try {
    window.localStorage.setItem(PENDING_QUERY_NODE_KEY, JSON.stringify(node));
  } catch {
    /* almacenamiento no disponible: se ignora silenciosamente */
  }
}

export function takePendingQueryNode(): PendingQueryNode | null {
  try {
    const raw = window.localStorage.getItem(PENDING_QUERY_NODE_KEY);
    if (!raw) return null;
    window.localStorage.removeItem(PENDING_QUERY_NODE_KEY);
    return JSON.parse(raw) as PendingQueryNode;
  } catch {
    return null;
  }
}
