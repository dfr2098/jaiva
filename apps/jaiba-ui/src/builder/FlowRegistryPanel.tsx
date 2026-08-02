import type { FlowRecord, FlowVersion, FlowVersionState } from "../types";

const STATE_LABEL: Record<FlowVersionState, string> = {
  DRAFT: "Borrador",
  VALIDATED: "Validada",
  DEPLOYED: "Desplegada",
  ARCHIVED: "Archivada",
};

function formatWhen(epoch?: number | null): string {
  if (!epoch) return "—";
  return new Date(epoch * 1000).toLocaleString("es-MX", {
    dateStyle: "short",
    timeStyle: "short",
  });
}

export type VersionBusy =
  | null
  | { version: number; action: "validate" | "deploy" | "start" | "archive" | "load" }
  | { action: "rollback" | "rollback-start" | "save-draft" | "refresh" };

interface FlowRegistryPanelProps {
  flowId: string;
  record: FlowRecord | null;
  busy: VersionBusy;
  onRefresh: () => void;
  onSaveDraft: () => void;
  onRollback: (start: boolean) => void;
  onLoadVersion: (version: number) => void;
  onValidateVersion: (version: number) => void;
  onDeployVersion: (version: number, start: boolean) => void;
  onArchiveVersion: (version: number) => void;
}

function versionBusy(
  busy: VersionBusy,
  version: number,
  action: "validate" | "deploy" | "start" | "archive" | "load",
): boolean {
  return (
    busy !== null &&
    "version" in busy &&
    busy.version === version &&
    busy.action === action
  );
}

export function FlowRegistryPanel({
  flowId,
  record,
  busy,
  onRefresh,
  onSaveDraft,
  onRollback,
  onLoadVersion,
  onValidateVersion,
  onDeployVersion,
  onArchiveVersion,
}: FlowRegistryPanelProps) {
  const versions = [...(record?.versions ?? [])].sort(
    (left, right) => right.version - left.version,
  );
  const canRollback = versions.some(
    (version) =>
      version.deployed_at != null &&
      (version.state === "ARCHIVED" || version.state === "DEPLOYED") &&
      version.version !== record?.active_version,
  );
  const locked = busy !== null;

  return (
    <div className="registry-panel">
      <div className="run-api-head">
        <span>Registro de versiones · {flowId}</span>
        <div className="registry-head-actions">
          <button
            type="button"
            className="button subtle"
            disabled={locked}
            onClick={onRefresh}
          >
            {busy !== null && "action" in busy && busy.action === "refresh"
              ? "…"
              : "Actualizar"}
          </button>
          <button
            type="button"
            className="button"
            disabled={locked}
            onClick={onSaveDraft}
          >
            {busy !== null && "action" in busy && busy.action === "save-draft"
              ? "Guardando…"
              : "Guardar borrador en registro"}
          </button>
        </div>
      </div>

      <p className="run-hint">
        Cada publicación crea una versión inmutable. Puedes validar, desplegar o
        restaurar una versión anterior sin perder el historial.
      </p>

      {record?.active_version != null ? (
        <p className="registry-active">
          Versión activa: <strong>v{record.active_version}</strong>
        </p>
      ) : (
        <p className="registry-active muted">Sin versión desplegada aún.</p>
      )}

      <div className="registry-rollback">
        <button
          type="button"
          className="button"
          disabled={!canRollback || locked}
          onClick={() => onRollback(false)}
        >
          {busy !== null && "action" in busy && busy.action === "rollback"
            ? "Restaurando…"
            : "Rollback detenido"}
        </button>
        <button
          type="button"
          className="button primary"
          disabled={!canRollback || locked}
          onClick={() => onRollback(true)}
        >
          {busy !== null &&
          "action" in busy &&
          busy.action === "rollback-start"
            ? "Restaurando…"
            : "Rollback e iniciar"}
        </button>
      </div>

      {versions.length === 0 ? (
        <p className="run-hint">
          Este flujo aún no está en el registro del motor. Publícalo o guarda un
          borrador para crear la primera versión.
        </p>
      ) : (
        <div className="registry-table-wrap">
          <table className="registry-table">
            <thead>
              <tr>
                <th>Ver.</th>
                <th>Estado</th>
                <th>Creada</th>
                <th>Acciones</th>
              </tr>
            </thead>
            <tbody>
              {versions.map((version) => (
                <VersionRow
                  key={version.version}
                  version={version}
                  active={version.version === record?.active_version}
                  locked={locked}
                  busy={busy}
                  onLoad={() => onLoadVersion(version.version)}
                  onValidate={() => onValidateVersion(version.version)}
                  onDeploy={(start) => onDeployVersion(version.version, start)}
                  onArchive={() => onArchiveVersion(version.version)}
                />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function VersionRow({
  version,
  active,
  locked,
  busy,
  onLoad,
  onValidate,
  onDeploy,
  onArchive,
}: {
  version: FlowVersion;
  active: boolean;
  locked: boolean;
  busy: VersionBusy;
  onLoad: () => void;
  onValidate: () => void;
  onDeploy: (start: boolean) => void;
  onArchive: () => void;
}) {
  return (
    <tr className={active ? "active" : undefined}>
      <td>
        <strong>v{version.version}</strong>
        {active ? <span className="registry-chip">activa</span> : null}
      </td>
      <td>
        <span className={`registry-state ${version.state.toLowerCase()}`}>
          {STATE_LABEL[version.state]}
        </span>
      </td>
      <td>{formatWhen(version.created_at)}</td>
      <td>
        <div className="registry-row-actions">
          <button
            type="button"
            className="button subtle"
            disabled={locked}
            onClick={onLoad}
            title="Cargar YAML de esta versión en el lienzo"
          >
            {versionBusy(busy, version.version, "load") ? "…" : "Abrir"}
          </button>
          {version.state === "DRAFT" ? (
            <button
              type="button"
              className="button subtle"
              disabled={locked}
              onClick={onValidate}
            >
              {versionBusy(busy, version.version, "validate")
                ? "…"
                : "Validar"}
            </button>
          ) : null}
          {version.state === "VALIDATED" || version.state === "DEPLOYED" ? (
            <>
              <button
                type="button"
                className="button subtle"
                disabled={locked}
                onClick={() => onDeploy(false)}
              >
                {versionBusy(busy, version.version, "deploy")
                  ? "…"
                  : "Desplegar"}
              </button>
              <button
                type="button"
                className="button subtle"
                disabled={locked}
                onClick={() => onDeploy(true)}
              >
                {versionBusy(busy, version.version, "start")
                  ? "…"
                  : "Iniciar"}
              </button>
            </>
          ) : null}
          {version.state !== "ARCHIVED" && !active ? (
            <button
              type="button"
              className="button subtle danger"
              disabled={locked}
              onClick={onArchive}
            >
              {versionBusy(busy, version.version, "archive")
                ? "…"
                : "Archivar"}
            </button>
          ) : null}
        </div>
      </td>
    </tr>
  );
}
