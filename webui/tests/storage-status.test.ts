import assert from "node:assert/strict";
import test from "node:test";
import { loadPoolStatus, parseAffectedStatus, parsePoolStatus, rpcFailure } from "../src/lib/storage-status.ts";
import type { Value } from "../src/lib/storage-status.ts";

function value<T>(result: Value<T>): T {
  assert.equal(result.state, "known");
  if (result.state !== "known") assert.fail(result.reason);
  return result.value;
}

const member = {
  id: "hdd-a",
  capacity_bytes: 10000,
  used_bytes: 3000,
  reserved_bytes: 100,
  available_bytes: 6900,
  objects: 3,
  pending_uploads: 1,
  cleanup_debts: 2,
  unreadable_objects: 0,
  evacuation: "idle",
  safe_to_remove: false,
  safe_to_pull_with_known_loss: false,
  readable_evacuated: false,
  health: {
    quarantine: null,
    monitoring_error: "SMART unavailable",
    warning: null,
    evacuation_error: null,
    observed_ms: null,
  },
};
const pool = {
  pool_id: "test-pool",
  members: [member],
  pending_operations: 1,
  mapped_objects: 3,
  unreadable: [],
  quarantined_leftovers: [],
};
const affected = {
  member: "hdd-a",
  complete: true,
  errors: [],
  coverage: "separate coherent metadata snapshots; unlinked/unreachable inodes have no paths",
  pathname_encoding: "absolute byte arrays",
  views: [
    {
      view: "live",
      unresolved_segments: [],
      references: [
        { inode: 4, extent: 0, segment: "segments/1", paths: [[47, 77, 111, 118, 105, 101]] },
        { inode: 4, extent: 1, segment: "segments/1", paths: [[47, 77, 111, 118, 105, 101]] },
      ],
    },
  ],
};

test("populated pool preserves configured, charged, reserved, and allocatable bytes distinctly", () => {
  const parsed = value(parsePoolStatus(JSON.stringify(pool)));
  assert.equal(value(parsed.id), "test-pool");
  assert.equal(value(parsed.pendingOperations), 1);
  assert.equal(value(parsed.unreadableObjects), 0);
  const disk = value(parsed.members)[0];
  assert.equal(value(disk.capacity), 10000);
  assert.equal(value(disk.used), 3000);
  assert.equal(value(disk.reserved), 100);
  assert.equal(value(disk.available), 6900);
  assert.equal(value(disk.safeToRemove), false);
  assert.equal(value(disk.monitoringError), "SMART unavailable");
  assert.equal(value(disk.quarantine), null);
  assert.equal(disk.observedMs.state, "unavailable");
  assert.deepEqual(disk.observedMs, { state: "unavailable", reason: "No health observation has been reported" });
});

test("only the explicit no-pool server result means no pool", async () => {
  const noPool = Object.assign(new Error("[failed_precondition] no permanent pool configured"), {
    code: 9,
    rawMessage: "no permanent pool configured",
  });
  const request = async () => {
    throw noPool;
  };
  assert.equal((await loadPoolStatus(request)).state, "no-pool");
  for (const error of [
    new Error("no permanent pool configured"),
    { code: 9, rawMessage: "catalog unavailable" },
    { code: 14, rawMessage: "no permanent pool configured" },
  ]) {
    assert.equal(
      (
        await loadPoolStatus(async () => {
          throw error;
        })
      ).state,
      "unavailable",
    );
  }
});

test("failed RPCs remain unavailable and never create an empty or destroyed pool", async () => {
  const result = await loadPoolStatus(async () => {
    throw new Error("connection refused");
  });
  assert.deepEqual(result, { state: "unavailable", reason: "connection refused" });
  assert.deepEqual(rpcFailure(new Error("scan timeout")), { noPool: false, reason: "scan timeout" });
});

test("successful empty member list is distinct from missing members and absent pool", async () => {
  const result = await loadPoolStatus(async () => ({ json: JSON.stringify({ ...pool, members: [] }) }));
  assert.equal(result.state, "pool");
  if (result.state !== "pool") assert.fail();
  assert.deepEqual(value(result.pool.members), []);
  assert.equal(value(parsePoolStatus("{}")).members.state, "unavailable");
  assert.equal(parsePoolStatus("null").state, "unavailable");
});

test("unknown fields are ignored while missing and invalid fields remain unavailable", () => {
  const parsed = value(
    parsePoolStatus(
      JSON.stringify({
        ...pool,
        future: { healthy: true },
        members: [
          {
            id: "a",
            capacity_bytes: -1,
            used_bytes: "5",
            reserved_bytes: 0,
            safe_to_remove: "yes",
            health: { quarantine: false },
          },
        ],
      }),
    ),
  );
  const disk = value(parsed.members)[0];
  assert.equal(value(disk.id), "a");
  assert.equal(value(disk.reserved), 0);
  for (const field of [
    disk.capacity,
    disk.used,
    disk.available,
    disk.safeToRemove,
    disk.quarantine,
    disk.monitoringError,
  ])
    assert.equal(field.state, "unavailable");
  assert.equal(value(parsed.mappedObjects), 3);
});

test("malformed JSON, invalid records, and unsafe JSON integers fail closed", () => {
  for (const json of ["{", "[]", '"pool"']) assert.equal(parsePoolStatus(json).state, "unavailable");
  assert.equal(value(parsePoolStatus(JSON.stringify({ members: [null] }))).members.state, "unavailable");
  assert.equal(
    value(value(parsePoolStatus(JSON.stringify({ members: [{ capacity_bytes: 2 ** 53 }] }))).members)[0].capacity.state,
    "unavailable",
  );
});

test("complete scans deduplicate paths per metadata view and retain unreachable references", () => {
  const parsed = value(parseAffectedStatus(JSON.stringify(affected), "hdd-a"));
  assert.equal(value(parsed.pathCount), 1);
  const empty = { ...affected, views: [{ view: "live", references: [], unresolved_segments: [] }] };
  assert.equal(value(value(parseAffectedStatus(JSON.stringify(empty), "hdd-a")).pathCount), 0);
  const unlinked = {
    ...affected,
    views: [
      {
        view: "live",
        references: [{ inode: 8, extent: 0, segment: "segments/1", paths: [] }],
        unresolved_segments: [],
      },
    ],
  };
  const refs = value(value(value(parseAffectedStatus(JSON.stringify(unlinked), "hdd-a")).views)[0].references);
  assert.deepEqual(value(refs[0].paths), []);
});

test("incomplete scans never report zero paths, even if the returned views are empty", () => {
  for (const partial of [
    { ...affected, complete: false, views: [], errors: ["live metadata incomplete"] },
    { ...affected, complete: false, views: [{ view: "live", references: [], unresolved_segments: [] }] },
    { ...affected, complete: undefined },
    { ...affected, errors: undefined },
    { ...affected, errors: ["checkpoint metadata incomplete"] },
    { ...affected, views: [] },
  ]) {
    const parsed = value(parseAffectedStatus(JSON.stringify(partial), "hdd-a"));
    assert.equal(parsed.pathCount.state, "unavailable");
  }
});

test("segments absent from one coherent view do not hide reported paths in another view", () => {
  const raw = {
    ...affected,
    views: [
      { view: "live", references: [], unresolved_segments: ["segments/checkpoint-only"] },
      {
        view: "checkpoint/archive",
        unresolved_segments: ["segments/waiting-for-gc"],
        references: [{ inode: 4, extent: 0, segment: "segments/checkpoint-only", paths: [[47, 97]] }],
      },
    ],
  };
  const parsed = value(parseAffectedStatus(JSON.stringify(raw), "hdd-a"));
  assert.equal(value(parsed.pathCount), 1);
  assert.deepEqual(value(value(parsed.views)[0].unresolvedSegments), ["segments/checkpoint-only"]);
});

test("non-UTF-8 and control-containing paths have reversible escaped displays and raw bytes", () => {
  const paths = [
    [47, 255, 92, 10],
    [47, 195, 169],
    [47, 97, 10, 92, 98],
  ];
  const raw = {
    ...affected,
    views: [
      { view: "live", unresolved_segments: [], references: [{ inode: 4, extent: 0, segment: "segments/1", paths }] },
    ],
  };
  const parsed = value(parseAffectedStatus(JSON.stringify(raw), "hdd-a"));
  const result = value(value(value(parsed.views)[0].references)[0].paths);
  assert.deepEqual(result[0], { display: "/\\xff\\x5c\\x0a", hex: "2f ff 5c 0a", utf8: false });
  assert.equal(result[1].display, "/é");
  assert.equal(result[2].display, "/a\\n\\\\b");
  assert.equal(value(parsed.pathCount), 3);
});

test("malformed byte paths and missing reference fields make the total unavailable", () => {
  for (const paths of [[[47, 256]], [[47, -1]], [[47, 0]], [[65]], [[47, "a"]], undefined]) {
    const raw = {
      ...affected,
      views: [
        { view: "live", unresolved_segments: [], references: [{ inode: 4, extent: 0, segment: "segments/1", paths }] },
      ],
    };
    const parsed = value(parseAffectedStatus(JSON.stringify(raw), "hdd-a"));
    assert.equal(parsed.pathCount.state, "unavailable");
  }
});

test("affected scans reject mismatched members, malformed payloads, and missing metadata", () => {
  assert.equal(parseAffectedStatus(JSON.stringify(affected), "other-member").state, "unavailable");
  assert.equal(parseAffectedStatus("{}", "hdd-a").state, "unavailable");
  assert.equal(parseAffectedStatus("{", "hdd-a").state, "unavailable");
  const parsed = value(parseAffectedStatus(JSON.stringify({ member: "hdd-a" }), "hdd-a"));
  for (const field of [parsed.complete, parsed.errors, parsed.views, parsed.pathCount, parsed.coverage])
    assert.equal(field.state, "unavailable");
});
