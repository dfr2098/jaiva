/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_JAIBA_API_BASE?: string;
  readonly VITE_JAIVA_API_BASE?: string;
  readonly TAURI_ENV_PLATFORM?: string;
  readonly TAURI_ENV_DEBUG?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
