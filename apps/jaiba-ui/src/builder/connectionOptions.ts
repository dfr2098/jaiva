import type { FieldDef } from "./catalog";
import type { DatabaseConnection as FlowDatabaseConnection } from "./model";
import type { DatabaseConnection as ManagedConnection } from "../types";

export type ConnectionKind = NonNullable<FieldDef["connectionKind"]>;

const SQL_TYPES = new Set(["postgres", "mysql", "mariadb", "oracle", "sqlserver"]);
const DB_TYPES = new Set([...SQL_TYPES, "mongodb"]);

function normalizeType(value: string): string {
  return value.trim().toLowerCase();
}

function matchesKind(connectionType: string, kind: ConnectionKind): boolean {
  const type = normalizeType(connectionType);
  switch (kind) {
    case "kafka":
      return type === "kafka";
    case "postgres":
      return type === "postgres";
    case "oracle":
      return type === "oracle";
    case "mongodb":
      return type === "mongodb";
    case "mysql":
      return type === "mysql" || type === "mariadb";
    case "sqlserver":
      return type === "sqlserver";
    case "database":
      // Escrituras SQL multi-motor (sin MongoDB).
      return SQL_TYPES.has(type);
    default:
      return true;
  }
}

/** Nombres locales del YAML + perfiles del Connection Manager, filtrados por kind. */
export function connectionNamesForKind(
  kind: ConnectionKind | undefined,
  localDatabase: FlowDatabaseConnection[],
  localKafka: { name: string }[],
  managed: ManagedConnection[],
): string[] {
  const names = new Set<string>();

  if (kind === "kafka") {
    for (const connection of localKafka) {
      if (connection.name.trim()) names.add(connection.name.trim());
    }
    return [...names].sort((a, b) => a.localeCompare(b));
  }

  const effectiveKind: ConnectionKind = kind ?? "database";

  for (const connection of localDatabase) {
    if (!connection.name.trim()) continue;
    if (matchesKind(connection.type || "postgres", effectiveKind)) {
      names.add(connection.name.trim());
    }
  }

  for (const profile of managed) {
    if (!profile.name.trim()) continue;
    if (matchesKind(String(profile.connection_type ?? ""), effectiveKind)) {
      names.add(profile.name.trim());
    }
  }

  return [...names].sort((a, b) => a.localeCompare(b));
}

/** Alias de BD (SQL + Mongo) que el runtime puede resolver vía Connection Manager. */
export function managedDatabaseAliases(managed: ManagedConnection[]): string[] {
  return managed
    .filter((profile) => DB_TYPES.has(normalizeType(String(profile.connection_type ?? ""))))
    .map((profile) => profile.name.trim())
    .filter(Boolean);
}
