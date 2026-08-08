import { expect, type APIRequestContext } from "@playwright/test";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
export const REPO_ROOT = path.resolve(here, "../../../..");

export const API_BASE =
  process.env.JAIBA_E2E_API_URL ?? "http://127.0.0.1:19090";
export const ADMIN_TOKEN =
  process.env.JAIBA_ADMIN_TOKEN ?? "jaiba-stable-admin-token";

export const PG = {
  host: process.env.JAIBA_E2E_PG_HOST ?? "postgres",
  port: Number(process.env.JAIBA_E2E_PG_PORT ?? "5432"),
  database: process.env.JAIBA_E2E_PG_DB ?? "jaiba_stable",
  username: process.env.JAIBA_E2E_PG_USER ?? "jaiba",
  password: process.env.JAIBA_E2E_PG_PASSWORD ?? "jaiba_stable",
};

export function authHeaders(
  extra: Record<string, string> = {},
): Record<string, string> {
  return {
    Authorization: `Bearer ${ADMIN_TOKEN}`,
    ...extra,
  };
}

export function readExample(name: string): string {
  return fs.readFileSync(path.join(REPO_ROOT, "examples", name), "utf8");
}

/** Sustituye `id:` y rutas `/data/<slug>/` para aislar corridas. */
export function materializeFlow(
  yaml: string,
  flowId: string,
  dataSlug?: string,
): string {
  let out = yaml.replace(/^id:\s*.+$/m, `id: ${flowId}`);
  if (dataSlug) {
    out = out.replace(/\/data\/[a-z0-9-]+\//g, `/data/${dataSlug}/`);
  }
  return out;
}

export async function apiJson<T>(
  request: APIRequestContext,
  method: string,
  urlPath: string,
  options: {
    body?: string;
    contentType?: string;
    expected?: number[];
  } = {},
): Promise<T> {
  const response = await request.fetch(`${API_BASE}${urlPath}`, {
    method,
    headers: authHeaders(
      options.body
        ? { "Content-Type": options.contentType ?? "application/json" }
        : {},
    ),
    data: options.body,
  });
  const allowed = options.expected ?? [200, 201];
  const text = await response.text();
  expect(
    allowed,
    `${method} ${urlPath} → ${response.status()}: ${text.slice(0, 400)}`,
  ).toContain(response.status());
  if (!text) return undefined as T;
  return JSON.parse(text) as T;
}

export async function waitForFlowState(
  request: APIRequestContext,
  flowId: string,
  states: string[],
  timeoutMs = 45_000,
): Promise<string> {
  const deadline = Date.now() + timeoutMs;
  let last = "";
  while (Date.now() < deadline) {
    const view = await apiJson<{
      runtime?: { control?: { state?: string } };
    }>(request, "GET", `/api/v1/flows/${encodeURIComponent(flowId)}`).catch(
      () => null,
    );
    const state = view?.runtime?.control?.state;
    last = state ?? last;
    if (state && states.includes(state)) return state;
    await new Promise((r) => setTimeout(r, 400));
  }
  throw new Error(
    `timeout esperando ${states.join("|")} en ${flowId} (último: ${last || "—"})`,
  );
}

export async function waitForDeadLetters(
  request: APIRequestContext,
  flowId: string,
  min = 1,
  timeoutMs = 60_000,
): Promise<Array<{ queue_id: string; error?: string | null }>> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const response = await request.fetch(
      `${API_BASE}/api/v1/dead-letter?flow=${encodeURIComponent(flowId)}&limit=50`,
      { headers: authHeaders() },
    );
    if (response.ok()) {
      const letters =
        (await response.json()) as Array<{
          queue_id: string;
          error?: string | null;
        }>;
      if (Array.isArray(letters) && letters.length >= min) return letters;
    }
    await new Promise((r) => setTimeout(r, 500));
  }
  throw new Error(`timeout esperando ≥${min} dead-letter en ${flowId}`);
}
