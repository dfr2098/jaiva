import { useState } from "react";
import type { FlowRecord } from "../types";

interface TopMenuProps {
  engineOnline: boolean;
  downloadDisabled: boolean;
  saveState: "saved" | "saving" | "unsaved";
  flowId: string;
  registeredFlows: FlowRecord[];
  onSelectFlow: (flowId: string) => void;
  onNewDraft: () => void;
  onViewYaml: () => void;
  onDownload: () => void;
  onImport: () => void;
  onSave: () => void;
  onClear: () => void;
  onRun: () => void;
  onShowLogs: () => void;
  onOpenSettings: () => void;
}

export function TopMenu({
  engineOnline,
  downloadDisabled,
  saveState,
  flowId,
  registeredFlows,
  onSelectFlow,
  onNewDraft,
  onViewYaml,
  onDownload,
  onImport,
  onSave,
  onClear,
  onRun,
  onShowLogs,
  onOpenSettings,
}: TopMenuProps) {
  const [open, setOpen] = useState(false);

  const runItem = (action: () => void) => {
    setOpen(false);
    action();
  };

  const knownIds = new Set(registeredFlows.map((flow) => flow.id));
  const options = [...registeredFlows].sort((left, right) =>
    left.id.localeCompare(right.id),
  );

  return (
    <div className="ide-menubar">
      <label className="flow-switcher">
        <span>Flujo</span>
        <select
          value={knownIds.has(flowId) ? flowId : ""}
          onChange={(event) => {
            const value = event.target.value;
            if (value === "__new__") {
              onNewDraft();
              return;
            }
            if (value) onSelectFlow(value);
          }}
          title="Cambiar entre flujos del registro"
        >
          {!knownIds.has(flowId) ? (
            <option value="">
              {flowId ? `${flowId} (local)` : "Borrador local"}
            </option>
          ) : null}
          {options.map((flow) => (
            <option key={flow.id} value={flow.id}>
              {flow.id}
              {flow.active_version != null ? ` · v${flow.active_version}` : ""}
            </option>
          ))}
          <option value="__new__">＋ Nuevo borrador local</option>
        </select>
      </label>

      <div className="menu-group">
        <button
          type="button"
          className={`menu-button ${open ? "open" : ""}`}
          onClick={() => setOpen((value) => !value)}
        >
          Proyecto
        </button>
        {open ? (
          <>
            <div className="menu-backdrop" onClick={() => setOpen(false)} />
            <div className="menu-dropdown">
              <button type="button" onClick={() => runItem(onImport)}>
                Importar YAML
              </button>
              <button type="button" onClick={() => runItem(onViewYaml)}>
                Ver YAML
              </button>
              <button
                type="button"
                disabled={downloadDisabled}
                onClick={() => runItem(onDownload)}
              >
                Descargar YAML
              </button>
              <button type="button" onClick={() => runItem(onSave)}>
                Guardar borrador local
              </button>
              <div className="menu-sep" />
              <button type="button" className="danger" onClick={() => runItem(onClear)}>
                Limpiar lienzo
              </button>
            </div>
          </>
        ) : null}
      </div>

      <button type="button" className="menu-button" onClick={onRun}>
        Ejecutar
      </button>
      <button type="button" className="menu-button" onClick={onShowLogs}>
        Logs
      </button>
      <button type="button" className="menu-button" onClick={onOpenSettings}>
        Configuración
      </button>

      <span className={`engine-pill ${engineOnline ? "online" : "offline"}`}>
        <i />
        {engineOnline ? "Motor en línea" : "Motor desconectado"}
      </span>
      <span className={`draft-state ${saveState}`}>
        {saveState === "saved"
          ? "Borrador guardado"
          : saveState === "saving"
            ? "Guardando…"
            : "Cambios pendientes"}
      </span>
    </div>
  );
}
