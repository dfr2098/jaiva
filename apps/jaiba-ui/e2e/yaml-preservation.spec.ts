import { test, expect } from "@playwright/test";
import { parse } from "yaml";
import { parseFlowYaml, toYaml } from "../src/builder/yaml";
import { RETRY_DEFAULTS, SCHEDULING_DEFAULTS, SIMULATION_DEFAULTS } from "../src/builder/model";

const source = `id: preservation
extension: {keep: true}
engine:
  state_file: custom-state.json
  domain_memory: {enabled: true, policy_file: memory.yaml}
  repository: {enabled: true, database_path: custom.db, content_path: content}
database_connections:
  db: {type: postgres, url_env: DB_URL, acquire_timeout_seconds: 9}
processors:
  - id: source
    type: generate_records
    extension: {keep: yes}
    config: {records: []}
  - id: sink
    type: log_records
connections:
  - {from: source, to: sink, relationship: success, extension: preserved}
`;

test("import/export preserves runtime options and empty records", () => {
  const flow = parseFlowYaml(source);
  expect(parse(toYaml(flow.meta, flow.nodes, flow.edges))).toEqual(parse(source));
});

test("editing known settings preserves unknown options and renamed processors", () => {
  const flow = parseFlowYaml(source);
  flow.meta.engine.repository_enabled = false;
  flow.meta.engine.memory_maximum_percent = 25;
  flow.nodes[0].data.processorId = "renamed";
  flow.nodes[0].data.config.records = [{ id: 1 }];
  const output = parse(toYaml(flow.meta, flow.nodes, flow.edges));
  expect(output.engine.repository).toEqual({ enabled: false, database_path: "custom.db", content_path: "content" });
  expect(output.engine.domain_memory).toEqual({ enabled: true, policy_file: "memory.yaml" });
  expect(output.engine.state_file).toBe("custom-state.json");
  expect(output.engine.memory.maximum_percent).toBe(25);
  expect(output.processors[0]).toMatchObject({ id: "renamed", extension: { keep: "yes" }, config: { records: [{ id: 1 }] } });
  expect(output.connections[0]).toMatchObject({ from: "renamed", extension: "preserved" });
  expect(output.database_connections.db.acquire_timeout_seconds).toBe(9);
});

test("deleting nodes and connections does not resurrect imported content", () => {
  const flow = parseFlowYaml(source);
  flow.nodes = flow.nodes.slice(0, 1);
  flow.edges = [];
  const output = parse(toYaml(flow.meta, flow.nodes, flow.edges));
  expect(output.processors).toHaveLength(1);
  expect(output.connections).toEqual([]);
  expect(output.engine.domain_memory.enabled).toBe(true);
});

for (const extensions of [true, false]) {
  test(`resetting controls to defaults preserves extensions: ${extensions}`, () => {
    const input = parse(source);
    const extension = extensions ? { extension: { keep: true } } : {};
    Object.assign(input.processors[0], {
      scheduling: { concurrent_tasks: 2, ...extension },
      retry: { maximum_attempts: 3, ...extension },
      simulation: { mode: "mock", options: { records: [{ id: 1 }] }, ...extension },
    });
    input.connections[0].queue = { capacity: 200, ...extension };
    const flow = parseFlowYaml(JSON.stringify(input));
    flow.nodes[0].data.scheduling = { ...SCHEDULING_DEFAULTS };
    flow.nodes[0].data.retry = { ...RETRY_DEFAULTS };
    flow.nodes[0].data.simulation = { ...SIMULATION_DEFAULTS };
    flow.edges[0].data!.queueCapacity = 100;
    const output = parse(toYaml(flow.meta, flow.nodes, flow.edges));
    for (const block of ["scheduling", "retry", "simulation"]) {
      expect(output.processors[0][block]).toEqual(extensions ? extension : undefined);
    }
    expect(output.connections[0].queue).toEqual(extensions ? extension : undefined);
    const reopened = parseFlowYaml(JSON.stringify(output));
    expect(reopened.nodes[0].data.scheduling).toEqual(SCHEDULING_DEFAULTS);
    expect(reopened.nodes[0].data.retry).toEqual(RETRY_DEFAULTS);
    expect(reopened.nodes[0].data.simulation).toEqual(SIMULATION_DEFAULTS);
    expect(reopened.edges[0].data!.queueCapacity).toBe(100);
  });
}

test("deleting connections and disabling the schedule removes their extensions too", () => {
  const input = parse(source);
  input.schedule = { enabled: true, trigger: { type: "interval", every_seconds: 60 }, extension: "remove" };
  const flow = parseFlowYaml(JSON.stringify(input));
  flow.meta.databaseConnections = [];
  flow.meta.schedule.enabled = false;
  const output = parse(toYaml(flow.meta, flow.nodes, flow.edges));
  expect(output.database_connections).toBeUndefined();
  expect(output.schedule).toBeUndefined();
});
