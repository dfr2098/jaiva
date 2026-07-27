import { useState } from "react";

interface TopMenuProps {
  engineOnline: boolean;
  downloadDisabled: boolean;
  saveState: "saved" | "saving" | "unsaved";
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

  return (
    <div className="ide-menubar">
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
        {saveState === "saved" ? "Borrador guardado" : saveState === "saving" ? "Guardando…" : "Cambios pendientes"}
      </span>
    </div>
  );
}
