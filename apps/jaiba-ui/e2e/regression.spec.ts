import { expect, test, type APIRequestContext, type Page } from "@playwright/test";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import {
  ADMIN_TOKEN,
  API_BASE,
  PG,
  REPO_ROOT,
  apiJson,
  authHeaders,
  materializeFlow,
  readExample,
  waitForDeadLetters,
  waitForFlowState,
} from "./helpers/api";

async function withAdminToken(page: Page) {
  await page.addInitScript((token) => {
    window.sessionStorage.setItem("jaiba.admin.token", token);
  }, ADMIN_TOKEN);
}

async function deployStart(
  request: APIRequestContext,
  flowId: string,
  yaml: string,
) {
  return apiJson<{
    flow_id: string;
    control: { state: string };
    metrics: { processed: number; failed: number };
  }>(request, "PUT", `/api/v1/flows/${encodeURIComponent(flowId)}?start=true`, {
    body: yaml,
    contentType: "application/yaml",
  });
}

async function mutate(
  request: APIRequestContext,
  flowId: string,
  action: string,
) {
  return apiJson<{ control: { state: string } }>(
    request,
    "POST",
    `/api/v1/flows/${encodeURIComponent(flowId)}/${action}`,
  );
}

function restartJaibaServer() {
  const envFile = path.join(REPO_ROOT, "deploy", ".env");
  const envExample = path.join(REPO_ROOT, "deploy", ".env.example");
  const env = fs.existsSync(envFile) ? envFile : envExample;
  execFileSync(
    "docker",
    [
      "compose",
      "--env-file",
      env,
      "-f",
      path.join(REPO_ROOT, "deploy", "docker-compose.release-core.yml"),
      "restart",
      "jaiba",
    ],
    { stdio: "inherit" },
  );
}

async function waitHealth(request: APIRequestContext, timeoutMs = 120_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await request.get(`${API_BASE}/health`);
      if (res.ok()) {
        const body = await res.json();
        if (body?.status === "ok") return;
      }
    } catch {
      // reinicio en curso (ECONNRESET / connection refused)
    }
    await new Promise((r) => setTimeout(r, 1000));
  }
  throw new Error("timeout esperando /health tras restart");
}

test.describe("regresión corta (API + UI)", () => {
  test("01) health API", async ({ request }) => {
    const res = await request.get(`${API_BASE}/health`);
    expect(res.ok()).toBeTruthy();
    await expect(res.json()).resolves.toMatchObject({ status: "ok" });
  });

  test("02) UI online", async ({ page }) => {
    await withAdminToken(page);
    await page.goto("/");
    const status = page.getByTestId("engine-status");
    // Tras recreate del server el proxy nginx puede necesitar un reload.
    for (let attempt = 0; attempt < 3; attempt += 1) {
      try {
        await expect(status).toHaveAttribute("data-online", "true", {
          timeout: 15_000,
        });
        return;
      } catch {
        await page.reload();
      }
    }
    await expect(status).toHaveAttribute("data-online", "true", {
      timeout: 30_000,
    });
  });

  test("03) validate examples/smoke.yaml", async ({ request }) => {
    const yaml = readExample("smoke.yaml");
    const result = await apiJson<{
      valid: boolean;
      flow_id: string;
      processors: number;
    }>(request, "POST", "/api/v1/flows/validate", {
      body: yaml,
      contentType: "application/yaml",
    });
    expect(result.valid).toBe(true);
    expect(result.flow_id).toBe("smoke");
    expect(result.processors).toBeGreaterThanOrEqual(3);
  });

  test("04) deploy + run flow básico (smoke)", async ({ request }) => {
    const flowId = `smoke-basic-${Date.now()}`;
    const yaml = materializeFlow(readExample("smoke.yaml"), flowId);
    // Rutas locales .jaiva → /data aislado en el contenedor
    const patched = yaml
      .replaceAll(".jaiva/smoke-repository.db", `/data/${flowId}/repository.db`)
      .replaceAll(".jaiva/smoke-content", `/data/${flowId}/content`)
      .replaceAll(".jaiva/smoke-logs", `/data/${flowId}/logs`);
    await deployStart(request, flowId, patched);

    const deadline = Date.now() + 45_000;
    let processed = 0;
    while (Date.now() < deadline) {
      const view = await apiJson<{
        runtime?: { metrics?: { processed?: number }; control?: { state?: string } };
      }>(request, "GET", `/api/v1/flows/${encodeURIComponent(flowId)}`);
      processed = view.runtime?.metrics?.processed ?? 0;
      if (processed >= 2) break;
      await new Promise((r) => setTimeout(r, 400));
    }
    expect(processed).toBeGreaterThanOrEqual(2);
  });

  test("05) pause / resume (flow continuo)", async ({ request }) => {
    const flowId = `smoke-cont-${Date.now()}`;
    const yaml = materializeFlow(
      readExample("smoke-continuous.yaml"),
      flowId,
      flowId,
    );
    await deployStart(request, flowId, yaml);
    await waitForFlowState(request, flowId, ["RUNNING"]);

    const paused = await mutate(request, flowId, "pause");
    expect(paused.control.state).toBe("PAUSED");
    await waitForFlowState(request, flowId, ["PAUSED"]);

    const resumed = await mutate(request, flowId, "resume");
    expect(resumed.control.state).toBe("RUNNING");
    await waitForFlowState(request, flowId, ["RUNNING"]);

    await mutate(request, flowId, "stop");
  });

  test("06) UI pause / resume", async ({ page, request }) => {
    const flowId = `smoke-ui-cont-${Date.now()}`;
    const yaml = materializeFlow(
      readExample("smoke-continuous.yaml"),
      flowId,
      flowId,
    );
    await deployStart(request, flowId, yaml);
    await waitForFlowState(request, flowId, ["RUNNING"]);

    await withAdminToken(page);
    await page.goto("/");
    await page.getByTestId("nav-monitor").click();
    const switcher = page.locator(".monitor-flow-switcher select");
    await expect(switcher).toBeVisible({ timeout: 20_000 });
    await switcher.selectOption(flowId);
    await page.locator(".flow-panel").getByRole("button", { name: "Actualizar" }).click();

    await expect(page.getByTestId("flow-action-pause")).toBeEnabled({
      timeout: 30_000,
    });
    await page.getByTestId("flow-action-pause").click();
    await expect
      .poll(async () => {
        const view = await apiJson<{ runtime?: { control?: { state?: string } } }>(
          request,
          "GET",
          `/api/v1/flows/${encodeURIComponent(flowId)}`,
        );
        return view.runtime?.control?.state;
      })
      .toBe("PAUSED");

    await expect(page.getByTestId("flow-action-resume")).toBeEnabled({
      timeout: 15_000,
    });
    await page.getByTestId("flow-action-resume").click();
    await expect
      .poll(async () => {
        const view = await apiJson<{ runtime?: { control?: { state?: string } } }>(
          request,
          "GET",
          `/api/v1/flows/${encodeURIComponent(flowId)}`,
        );
        return view.runtime?.control?.state;
      })
      .toBe("RUNNING");

    await mutate(request, flowId, "stop");
  });

  test("07) fail → DLQ", async ({ request }) => {
    const flowId = `smoke-dlq-${Date.now()}`;
    const yaml = materializeFlow(readExample("smoke-dlq.yaml"), flowId, flowId);
    await deployStart(request, flowId, yaml);
    const letters = await waitForDeadLetters(request, flowId, 1);
    expect(letters[0].queue_id).toBeTruthy();
    expect(String(letters[0].error ?? "")).toMatch(/encoded|write_file|fail/i);
  });

  test("08) replay dead-letter", async ({ request }) => {
    const flowId = `smoke-dlq-replay-${Date.now()}`;
    const yaml = materializeFlow(readExample("smoke-dlq.yaml"), flowId, flowId);
    await deployStart(request, flowId, yaml);
    const letters = await waitForDeadLetters(request, flowId, 1);
    const replay = await apiJson<{ message: string }>(
      request,
      "POST",
      `/api/v1/dead-letter/${encodeURIComponent(letters[0].queue_id)}/replay?flow=${encodeURIComponent(flowId)}`,
    );
    expect(replay.message).toMatch(/requeued/i);
  });

  test("09) connection create + test (API)", async ({ request }) => {
    const created = await apiJson<{
      id: string;
      name: string;
      password?: string;
      status: { availability: string };
    }>(request, "POST", "/api/v1/connections", {
      body: JSON.stringify({
        name: `api-pg-${Date.now()}`,
        connection_type: "postgres",
        host: PG.host,
        port: PG.port,
        database: PG.database,
        username: PG.username,
        password: PG.password,
        ssl: false,
        pool_min: 1,
        pool_max: 4,
        timeout_ms: 10_000,
      }),
    });
    expect(created.id).toBeTruthy();
    // El secreto no vuelve en la vista (campo password ausente).
    expect(created.password).toBeUndefined();
    expect(JSON.stringify(created)).not.toMatch(/"password"\s*:/);

    const tested = await apiJson<{ status: { availability: string } }>(
      request,
      "POST",
      `/api/v1/connections/${encodeURIComponent(created.id)}/test`,
    );
    expect(tested.status.availability).toBe("available");
  });

  test("10) connection test falla con password mala", async ({ request }) => {
    const created = await apiJson<{ id: string }>(request, "POST", "/api/v1/connections", {
      body: JSON.stringify({
        name: `api-bad-${Date.now()}`,
        connection_type: "postgres",
        host: PG.host,
        port: PG.port,
        database: PG.database,
        username: PG.username,
        password: "wrong-password-regression",
        ssl: false,
        pool_min: 1,
        pool_max: 2,
        timeout_ms: 5_000,
      }),
    });
    const res = await request.fetch(
      `${API_BASE}/api/v1/connections/${encodeURIComponent(created.id)}/test`,
      { method: "POST", headers: authHeaders() },
    );
    // Puede ser 200 con unavailable o 4xx/5xx con error.
    if (res.ok()) {
      const body = await res.json();
      expect(body.status.availability).not.toBe("available");
    } else {
      expect(res.status()).toBeGreaterThanOrEqual(400);
    }
  });

  test("11) secreto persiste con master key (restart)", async ({ request }) => {
    test.setTimeout(180_000);
    const name = `persist-${Date.now()}`;
    const created = await apiJson<{ id: string; name: string }>(
      request,
      "POST",
      "/api/v1/connections",
      {
        body: JSON.stringify({
          name,
          connection_type: "postgres",
          host: PG.host,
          port: PG.port,
          database: PG.database,
          username: PG.username,
          password: PG.password,
          ssl: false,
          pool_min: 1,
          pool_max: 4,
          timeout_ms: 10_000,
        }),
      },
    );

    restartJaibaServer();
    await new Promise((r) => setTimeout(r, 2000));
    await waitHealth(request);

    const listed = await apiJson<Array<{ id: string; name: string }>>(
      request,
      "GET",
      "/api/v1/connections",
    );
    expect(listed.some((c) => c.id === created.id && c.name === name)).toBeTruthy();

    const tested = await apiJson<{ status: { availability: string } }>(
      request,
      "POST",
      `/api/v1/connections/${encodeURIComponent(created.id)}/test`,
    );
    expect(tested.status.availability).toBe("available");
  });

  test("12) UI crear Postgres + probar", async ({ page }) => {
    await withAdminToken(page);
    await page.goto("/");
    await page.getByTestId("nav-connections").click();
    const name = `ui-pg-${Date.now()}`;
    await page.getByTestId("connection-new").click();
    await page.getByTestId("driver-postgres").click();
    await page.getByTestId("connection-name").fill(name);
    await page.getByTestId("connection-host").fill(PG.host);
    await page.getByTestId("connection-port").fill(String(PG.port));
    await page.getByTestId("connection-database").fill(PG.database);
    await page.getByTestId("connection-username").fill(PG.username);
    await page.getByTestId("connection-password").fill(PG.password);
    await page.getByTestId("connection-save-test").click();
    await expect(page.getByTestId("connection-availability")).toHaveText(
      /Disponible/i,
      { timeout: 45_000 },
    );
  });

  test("13) whoami con bearer", async ({ request }) => {
    const me = await apiJson<{ actor: string; role: string }>(
      request,
      "GET",
      "/api/v1/whoami",
    );
    expect(me.role).toMatch(/admin/i);
  });

  test("14) API sin token rechaza connections", async ({ request }) => {
    const res = await request.get(`${API_BASE}/api/v1/connections`);
    expect([401, 403]).toContain(res.status());
  });
});
