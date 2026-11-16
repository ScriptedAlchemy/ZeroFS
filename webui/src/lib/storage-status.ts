import { Code } from "@connectrpc/connect";

export type Value<T> = { state: "known"; value: T } | { state: "unavailable"; reason: string };

export const known = <T>(value: T): Value<T> => ({ state: "known", value });
export const unavailable = (reason: string): { state: "unavailable"; reason: string } => ({
  state: "unavailable",
  reason,
});

type JsonObject = Record<string, unknown>;
const object = (value: unknown): JsonObject | undefined =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? (value as JsonObject) : undefined;

function field<T>(row: JsonObject, key: string, accepts: (value: unknown) => value is T): Value<T> {
  if (!(key in row)) return unavailable(`${key} was not reported by the server`);
  return accepts(row[key]) ? known(row[key]) : unavailable(`${key} has an unsupported value`);
}

const text = (value: unknown): value is string => typeof value === "string";
const count = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
const flag = (value: unknown): value is boolean => typeof value === "boolean";
const nullableText = (value: unknown): value is string | null => value === null || text(value);

function list<T>(row: JsonObject, key: string, parse: (value: unknown) => Value<T>): Value<T[]> {
  const raw = row[key];
  if (!Array.isArray(raw)) return unavailable(`${key} was not reported as an array`);
  const result: T[] = [];
  for (const item of raw) {
    const parsed = parse(item);
    if (parsed.state === "unavailable") return unavailable(`${key}: ${parsed.reason}`);
    result.push(parsed.value);
  }
  return known(result);
}

const stringValue = (raw: unknown): Value<string> => (text(raw) ? known(raw) : unavailable("expected text"));

export interface MemberStatus {
  id: Value<string>;
  capacity: Value<number>;
  used: Value<number>;
  reserved: Value<number>;
  available: Value<number>;
  objects: Value<number>;
  pendingUploads: Value<number>;
  cleanupDebts: Value<number>;
  unreadableObjects: Value<number>;
  evacuation: Value<string>;
  safeToRemove: Value<boolean>;
  safeToPullWithKnownLoss: Value<boolean>;
  readableEvacuated: Value<boolean>;
  quarantine: Value<string | null>;
  monitoringError: Value<string | null>;
  warning: Value<string | null>;
  evacuationError: Value<string | null>;
  observedMs: Value<number>;
}

function member(raw: unknown): Value<MemberStatus> {
  const row = object(raw);
  if (!row) return unavailable("invalid member record");
  const health = object(row.health);
  const healthText = (key: string) =>
    health ? field(health, key, nullableText) : unavailable("health was not reported");
  return known({
    id: field(row, "id", text),
    capacity: field(row, "capacity_bytes", count),
    used: field(row, "used_bytes", count),
    reserved: field(row, "reserved_bytes", count),
    available: field(row, "available_bytes", count),
    objects: field(row, "objects", count),
    pendingUploads: field(row, "pending_uploads", count),
    cleanupDebts: field(row, "cleanup_debts", count),
    unreadableObjects: field(row, "unreadable_objects", count),
    evacuation: field(row, "evacuation", text),
    safeToRemove: field(row, "safe_to_remove", flag),
    safeToPullWithKnownLoss: field(row, "safe_to_pull_with_known_loss", flag),
    readableEvacuated: field(row, "readable_evacuated", flag),
    quarantine: healthText("quarantine"),
    monitoringError: healthText("monitoring_error"),
    warning: healthText("warning"),
    evacuationError: healthText("evacuation_error"),
    observedMs:
      health?.observed_ms === null
        ? unavailable("No health observation has been reported")
        : health
          ? field(health, "observed_ms", count)
          : unavailable("health was not reported"),
  });
}

export interface PoolStatus {
  id: Value<string>;
  members: Value<MemberStatus[]>;
  pendingOperations: Value<number>;
  mappedObjects: Value<number>;
  unreadableObjects: Value<number>;
  quarantinedLeftovers: Value<number>;
}

function arrayLength(row: JsonObject, key: string): Value<number> {
  return Array.isArray(row[key]) ? known(row[key].length) : unavailable(`${key} was not reported as an array`);
}

export function parsePoolStatus(json: string): Value<PoolStatus> {
  let raw: unknown;
  try {
    raw = JSON.parse(json);
  } catch {
    return unavailable("Pool status is not valid JSON");
  }
  const row = object(raw);
  if (!row) return unavailable("Pool status is not an object");
  return known({
    id: field(row, "pool_id", text),
    members: list(row, "members", member),
    pendingOperations: field(row, "pending_operations", count),
    mappedObjects: field(row, "mapped_objects", count),
    unreadableObjects: arrayLength(row, "unreadable"),
    quarantinedLeftovers: arrayLength(row, "quarantined_leftovers"),
  });
}

export type PoolResult =
  | { state: "pool"; pool: PoolStatus }
  | { state: "no-pool"; reason: string }
  | { state: "unavailable"; reason: string };

export function rpcFailure(error: unknown): { reason: string; noPool: boolean } {
  const row = object(error);
  const reason = error instanceof Error ? error.message : "Admin RPC failed";
  return { reason, noPool: row?.code === Code.FailedPrecondition && row.rawMessage === "no permanent pool configured" };
}

export async function loadPoolStatus(request: () => Promise<{ json: string }>): Promise<PoolResult> {
  try {
    const result = parsePoolStatus((await request()).json);
    return result.state === "known" ? { state: "pool", pool: result.value } : result;
  } catch (error) {
    const failure = rpcFailure(error);
    return failure.noPool ? { state: "no-pool", reason: failure.reason } : unavailable(failure.reason);
  }
}

export interface BytePath {
  display: string;
  hex: string;
  utf8: boolean;
}

function bytePath(raw: unknown): Value<BytePath> {
  if (!Array.isArray(raw) || !raw.every((byte) => count(byte) && byte <= 255) || raw[0] !== 47 || raw.includes(0)) {
    return unavailable("path is not an absolute pathname byte array");
  }
  const bytes = Uint8Array.from(raw);
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join(" ");
  try {
    const decoded = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes);
    const display = JSON.stringify(decoded)
      .slice(1, -1)
      .replace(/[\u007f-\u009f]/g, (char) => `\\u${char.charCodeAt(0).toString(16).padStart(4, "0")}`);
    return known({ display, hex, utf8: true });
  } catch {
    const display = Array.from(bytes, (byte) =>
      byte >= 32 && byte <= 126 && byte !== 92 ? String.fromCharCode(byte) : `\\x${byte.toString(16).padStart(2, "0")}`,
    ).join("");
    return known({ display, hex, utf8: false });
  }
}

export interface AffectedReference {
  inode: Value<number>;
  extent: Value<number>;
  segment: Value<string>;
  paths: Value<BytePath[]>;
}

export interface AffectedView {
  name: Value<string>;
  references: Value<AffectedReference[]>;
  unresolvedSegments: Value<string[]>;
}

export interface AffectedStatus {
  member: Value<string>;
  complete: Value<boolean>;
  errors: Value<string[]>;
  coverage: Value<string>;
  views: Value<AffectedView[]>;
  pathCount: Value<number>;
}

function reference(raw: unknown): Value<AffectedReference> {
  const row = object(raw);
  return row
    ? known({
        inode: field(row, "inode", count),
        extent: field(row, "extent", count),
        segment: field(row, "segment", text),
        paths: list(row, "paths", bytePath),
      })
    : unavailable("invalid reference record");
}

function view(raw: unknown): Value<AffectedView> {
  const row = object(raw);
  return row
    ? known({
        name: field(row, "view", text),
        references: list(row, "references", reference),
        unresolvedSegments: list(row, "unresolved_segments", stringValue),
      })
    : unavailable("invalid view record");
}

function affectedCount(status: Omit<AffectedStatus, "pathCount">): Value<number> {
  if (status.complete.state !== "known" || !status.complete.value)
    return unavailable("Affected-path scan is incomplete or its completeness was not reported");
  if (status.errors.state !== "known" || status.errors.value.length)
    return unavailable("Affected-path scan errors are present or unavailable");
  if (status.views.state !== "known" || !status.views.value.length)
    return unavailable("No coherent metadata views were reported");
  let total = 0;
  for (const item of status.views.value) {
    if (item.name.state !== "known" || item.references.state !== "known" || item.unresolvedSegments.state !== "known")
      return unavailable("A metadata view is incomplete");
    const paths = new Set<string>();
    for (const ref of item.references.value) {
      if (
        ref.inode.state !== "known" ||
        ref.extent.state !== "known" ||
        ref.segment.state !== "known" ||
        ref.paths.state !== "known"
      )
        return unavailable("An affected reference is incomplete");
      for (const path of ref.paths.value) paths.add(path.hex);
    }
    total += paths.size;
  }
  return known(total);
}

export function parseAffectedStatus(json: string, expectedMember: string): Value<AffectedStatus> {
  let raw: unknown;
  try {
    raw = JSON.parse(json);
  } catch {
    return unavailable("Affected-path status is not valid JSON");
  }
  const row = object(raw);
  if (!row) return unavailable("Affected-path status is not an object");
  const memberId = field(row, "member", text);
  if (memberId.state !== "known" || memberId.value !== expectedMember)
    return unavailable("Affected-path response does not identify the requested member");
  const status = {
    member: memberId,
    complete: field(row, "complete", flag),
    errors: list(row, "errors", stringValue),
    coverage: field(row, "coverage", text),
    views: list(row, "views", view),
  };
  return known({ ...status, pathCount: affectedCount(status) });
}

export interface CapacityBreakdown {
  capacity: number;
  used: number;
  reserved: number;
  available: number;
  /** Capacity that is neither charged, reserved nor allocatable, such as the free-space reserve. */
  held: number;
}

export function capacityBreakdown(member: MemberStatus): Value<CapacityBreakdown> {
  const { capacity, used, reserved, available } = member;
  for (const part of [capacity, used, reserved, available]) if (part.state === "unavailable") return part;
  if (capacity.state !== "known" || used.state !== "known" || reserved.state !== "known" || available.state !== "known")
    return unavailable("capacity was not reported");
  if (capacity.value === 0) return unavailable("configured capacity is zero");
  const held = capacity.value - used.value - reserved.value - available.value;
  if (held < 0) return unavailable("reported usage exceeds the configured capacity");
  return known({
    capacity: capacity.value,
    used: used.value,
    reserved: reserved.value,
    available: available.value,
    held,
  });
}

export interface MemberHealth {
  issues: { label: string; text: string }[];
  unavailable: { label: string; reason: string }[];
}

/** Sorts the four health observations into reported problems and observations the server did not supply. */
export function memberHealth(member: MemberStatus): MemberHealth {
  const health: MemberHealth = { issues: [], unavailable: [] };
  for (const [label, value] of [
    ["Quarantine", member.quarantine],
    ["Monitoring error", member.monitoringError],
    ["Warning", member.warning],
    ["Evacuation error", member.evacuationError],
  ] as const) {
    if (value.state === "unavailable") health.unavailable.push({ label, reason: value.reason });
    else if (value.value !== null) health.issues.push({ label, text: value.value });
  }
  return health;
}

export interface AffectedPath {
  path: BytePath;
  extents: number;
}

export interface ViewSummary {
  paths: AffectedPath[];
  /** References whose inode has no reachable pathname in this view. */
  unreachableReferences: number;
}

/** Collapses a view's per-extent references into one row per pathname. */
export function summarizeView(view: AffectedView): Value<ViewSummary> {
  if (view.references.state === "unavailable") return view.references;
  const paths = new Map<string, AffectedPath>();
  let unreachableReferences = 0;
  for (const ref of view.references.value) {
    if (ref.paths.state === "unavailable") return ref.paths;
    if (ref.paths.value.length === 0) unreachableReferences += 1;
    for (const path of ref.paths.value) {
      const seen = paths.get(path.hex);
      if (seen) seen.extents += 1;
      else paths.set(path.hex, { path, extents: 1 });
    }
  }
  return known({ paths: [...paths.values()], unreachableReferences });
}
