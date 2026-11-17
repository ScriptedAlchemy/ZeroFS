import { useEffect, useState } from "react";
import type { ReactNode } from "react";
import { CircleAlert, CircleCheck, CircleHelp, HardDrive, Lock, RefreshCw, Search } from "lucide-react";
import { useTitle } from "../hooks/use-title";
import { useStorageActivity, useStoragePool } from "../hooks/use-storage";
import { adminClient } from "../lib/grpc/client";
import { binaryBytesParts } from "../lib/format";
import {
  capacityBreakdown,
  known,
  memberHealth,
  parseAffectedStatus,
  rpcFailure,
  summarizeView,
  unavailable,
} from "../lib/storage-status";
import type { AffectedStatus, AffectedView, MemberStatus, SegmentFault, Value } from "../lib/storage-status";
import { Button } from "../components/ui/Button";

// A larger response cannot be parsed and drawn without freezing the tab. The server sends one
// reference per extent today, so real pools exceed this until the admin API pages its answer.
const MAX_AFFECTED_JSON_CHARS = 8 * 1024 * 1024;
const MAX_PATHS_SHOWN = 200;
// A member can carry thousands of fault records; drawing them all would stall the page.
const MAX_FAULTS_SHOWN = 50;

const sectionTitle = "text-xs font-medium uppercase tracking-wider text-muted-foreground";
const count = (value: number | bigint) => value.toLocaleString("en-US");
const yesNo = (value: boolean) => (value ? "Yes" : "No");
const words = (value: string) => value.replaceAll("_", " ");

function Bytes({ value }: { value: number | bigint }) {
  const { short, exact } = binaryBytesParts(value);
  return (
    <>
      <span className="whitespace-nowrap">{short}</span>
      {exact !== null && (
        <>
          {" "}
          <span className="whitespace-nowrap text-xs text-muted-foreground">({exact})</span>
        </>
      )}
    </>
  );
}

function Fact<T>({ value, children }: { value: Value<T>; children?: (value: T) => ReactNode }) {
  if (value.state === "known") return <>{children ? children(value.value) : String(value.value)}</>;
  return (
    <span className="font-sans text-muted-foreground">
      <span className="text-muted">Unavailable</span>
      <span className="text-xs"> · {value.reason}</span>
    </span>
  );
}

function Row<T>(props: { label: ReactNode; value: Value<T>; children?: (value: T) => ReactNode }) {
  return (
    <div className="flex flex-col gap-0.5 py-1.5 sm:flex-row sm:items-baseline sm:justify-between sm:gap-6">
      <dt className="text-muted">{props.label}</dt>
      <dd className="font-mono tabular-nums sm:text-right">
        <Fact value={props.value}>{props.children}</Fact>
      </dd>
    </div>
  );
}

function Stat<T>(props: { label: string; value: Value<T>; children?: (value: T) => ReactNode }) {
  return (
    <div className="card-surface min-w-0 rounded-lg p-4">
      <dt className="mb-1.5 text-[11px] font-medium uppercase leading-tight tracking-wider text-muted-foreground">
        {props.label}
      </dt>
      <dd
        className={`font-mono tabular-nums tracking-tight ${props.value.state === "known" ? "text-xl font-semibold" : "text-sm"}`}
      >
        <Fact value={props.value}>{props.children}</Fact>
      </dd>
    </div>
  );
}

const capacityParts = [
  { key: "used", label: "Charged usage", color: "bg-primary" },
  { key: "reserved", label: "Reserved uploads", color: "bg-warning" },
  { key: "available", label: "Allocatable", color: "bg-border-bright" },
] as const;

function Capacity({ member }: { member: MemberStatus }) {
  const breakdown = capacityBreakdown(member);
  return (
    <div className="space-y-3">
      {breakdown.state === "known" ? (
        <div
          className="flex h-2.5 overflow-hidden rounded-full bg-background ring-1 ring-border"
          role="img"
          aria-label="Share of configured capacity that is charged, reserved, allocatable and held back"
        >
          {capacityParts.map(
            (part) =>
              breakdown.value[part.key] > 0 && (
                <div
                  key={part.key}
                  className={`${part.color} min-w-[3px]`}
                  style={{ width: `${(breakdown.value[part.key] / breakdown.value.capacity) * 100}%` }}
                />
              ),
          )}
        </div>
      ) : (
        <p className="text-xs text-muted-foreground">Capacity bar unavailable · {breakdown.reason}</p>
      )}
      <dl className="text-sm">
        <Row label="Configured capacity" value={member.capacity}>
          {(bytes) => <Bytes value={bytes} />}
        </Row>
        {capacityParts.map((part) => (
          <Row
            key={part.key}
            label={
              <span className="flex items-center gap-2">
                <span className={`h-2 w-2 rounded-sm ${part.color}`} />
                {part.label}
              </span>
            }
            value={member[part.key]}
          >
            {(bytes) => <Bytes value={bytes} />}
          </Row>
        ))}
        {breakdown.state === "known" && breakdown.value.held > 0 && (
          <Row
            label={
              <span className="flex items-center gap-2">
                <span className="h-2 w-2 rounded-sm bg-background ring-1 ring-border" />
                Held back
              </span>
            }
            value={known(breakdown.value.held)}
          >
            {(bytes) => <Bytes value={bytes} />}
          </Row>
        )}
      </dl>
    </div>
  );
}

const serverTime = (ms: number) => new Date(ms).toLocaleString();

function FaultList({ faults, label }: { faults: SegmentFault[]; label: string }) {
  if (faults.length === 0) return null;
  const hidden = faults.length - MAX_FAULTS_SHOWN;
  return (
    <div className="space-y-1">
      <ul className="divide-y divide-border/50 text-xs" aria-label={label}>
        {faults.slice(0, MAX_FAULTS_SHOWN).map((fault) => (
          <li key={fault.location} className="space-y-0.5 py-1.5">
            <p className="break-all font-mono">{fault.location}</p>
            <p className="text-muted-foreground">
              <span className="text-destructive">{fault.reason}</span> · {count(fault.attempts)}{" "}
              {fault.attempts === 1 ? "attempt" : "attempts"} · next retry {serverTime(fault.nextRetryMs)}
            </p>
          </li>
        ))}
      </ul>
      {hidden > 0 && (
        <p className="text-xs text-muted-foreground">
          and {count(hidden)} more {hidden === 1 ? "fault" : "faults"} not listed
        </p>
      )}
    </div>
  );
}

function HealthSummary({ member }: { member: MemberStatus }) {
  const health = memberHealth(member);
  // Green needs a completed observation, every field reported, and every monitor running.
  const observed = member.observedMs.state === "known";
  const limited = health.limits.length > 0;
  const unknown = health.unavailable.length > 0 || !observed;
  const tone = health.issues.length
    ? "text-destructive"
    : limited
      ? "text-warning"
      : unknown
        ? "text-muted"
        : "text-success";
  const Icon = health.issues.length || limited ? CircleAlert : unknown ? CircleHelp : CircleCheck;
  const headline = health.issues.length
    ? `${health.issues.length} health ${health.issues.length === 1 ? "issue" : "issues"} reported`
    : limited
      ? "Monitoring or evacuation is limited"
      : health.unavailable.length
        ? "Health not fully reported"
        : observed
          ? "No health issues reported"
          : "Health not observed yet";
  return (
    <div className="space-y-2 text-sm">
      <p className={`flex items-center gap-2 ${tone}`}>
        <Icon size={15} />
        {headline}
      </p>
      {health.issues.map((issue) => (
        <p key={issue.label} className="pl-6 text-destructive">
          <span className="text-muted">{issue.label}:</span> {issue.text}
        </p>
      ))}
      {member.quarantine.state === "known" && member.quarantine.value !== null && (
        <p className="pl-6 text-xs text-muted-foreground">
          Quarantined since{" "}
          <Fact value={member.quarantineSinceMs}>
            {(since) => (since === null ? "an unrecorded time" : serverTime(since))}
          </Fact>
        </p>
      )}
      {member.faults.state === "known" && (
        <div className="pl-6">
          <FaultList faults={member.faults.value} label="Unreadable segment faults" />
        </div>
      )}
      {health.limits.map((limit) => (
        <p key={limit.label} className="pl-6 text-warning">
          <span className="text-muted">{limit.label}:</span> {limit.text}
        </p>
      ))}
      {member.kernelCoverage.state === "known" && member.kernelCoverage.value !== null && !limited && (
        <p className="pl-6 text-xs text-muted-foreground">Kernel error coverage: {member.kernelCoverage.value}</p>
      )}
      {health.unavailable.map((item) => (
        <p key={item.label} className="pl-6 text-xs text-muted-foreground">
          {item.label} unavailable · {item.reason}
        </p>
      ))}
      {member.observedMs.state === "known" && (
        <p className="pl-6 text-xs text-muted-foreground">
          Last health observation {new Date(member.observedMs.value).toLocaleString()}
        </p>
      )}
    </div>
  );
}

function Member({ member, onScan }: { member: MemberStatus; onScan: (id: string) => void }) {
  const id = member.id;
  return (
    <section className="card-surface space-y-5 rounded-lg p-5">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h3 className="flex min-w-0 items-center gap-2 font-medium">
          <HardDrive size={17} className="shrink-0 text-muted" />
          <span className="truncate">
            <Fact value={id} />
          </span>
        </h3>
        <Button
          variant="outline"
          disabled={id.state !== "known"}
          onClick={() => {
            if (id.state === "known") onScan(id.value);
          }}
        >
          <span className="flex items-center gap-2 whitespace-nowrap">
            <Search size={14} />
            Inspect affected paths
          </span>
        </Button>
      </div>
      <Capacity member={member} />
      <HealthSummary member={member} />
      <dl className="grid grid-cols-2 gap-x-6 border-t border-border pt-3 text-sm">
        <Row label="Objects" value={member.objects}>
          {count}
        </Row>
        <Row label="Pending uploads" value={member.pendingUploads}>
          {count}
        </Row>
        <Row label="Cleanup debts" value={member.cleanupDebts}>
          {count}
        </Row>
        <Row label="Unreadable objects" value={member.unreadableObjects}>
          {count}
        </Row>
      </dl>
      <details className="text-sm">
        <summary className="cursor-pointer text-muted hover:text-foreground">Evacuation and removal</summary>
        <dl className="pt-2">
          <Row label="Evacuation state" value={member.evacuation}>
            {words}
          </Row>
          <Row label="Readable data evacuated" value={member.readableEvacuated}>
            {yesNo}
          </Row>
          <Row label="Evacuation capacity sufficient" value={member.evacuationCapacitySufficient}>
            {yesNo}
          </Row>
          <Row label="Unavailable members" value={member.unavailableMembers}>
            {(ids) => (ids.length ? ids.join(", ") : "None")}
          </Row>
          <Row label="Safe to remove" value={member.safeToRemove}>
            {yesNo}
          </Row>
          <Row label="Safe to pull with known loss" value={member.safeToPullWithKnownLoss}>
            {yesNo}
          </Row>
        </dl>
        <p className="mt-2 text-xs text-muted-foreground">
          These are existing pool observations. They do not establish whole-file relocation or metadata survival after
          device loss.
        </p>
      </details>
    </section>
  );
}

function ViewDetails({ view }: { view: AffectedView }) {
  const summary = summarizeView(view);
  return (
    <div className="space-y-2 border-t border-border pt-3">
      <h4 className="font-medium">
        <Fact value={view.name} />
      </h4>
      {view.unresolvedSegments.state === "unavailable" ? (
        <p className="text-xs">
          Unresolved segments: <Fact value={view.unresolvedSegments} />
        </p>
      ) : (
        view.unresolvedSegments.value.length > 0 && (
          <p className="break-words text-xs text-warning">
            Unresolved segments: {view.unresolvedSegments.value.join(", ")}
          </p>
        )
      )}
      {summary.state === "unavailable" ? (
        <Fact value={summary} />
      ) : (
        <>
          {summary.value.unreachableReferences > 0 && (
            <p className="text-xs text-muted-foreground">
              {count(summary.value.unreachableReferences)} references have no reachable pathname.
            </p>
          )}
          <ul className="divide-y divide-border/50 text-sm">
            {summary.value.paths.slice(0, MAX_PATHS_SHOWN).map(({ path, extents }) => (
              <li key={path.hex} className="flex items-baseline justify-between gap-4 py-1.5">
                <span className="break-all font-mono" title={`Path bytes (hex): ${path.hex}`}>
                  {path.display}
                  {!path.utf8 && (
                    <span className="ml-2 font-sans text-xs text-muted-foreground">non-UTF-8; escaped bytes</span>
                  )}
                </span>
                <span className="shrink-0 whitespace-nowrap text-xs tabular-nums text-muted-foreground">
                  {count(extents)} {extents === 1 ? "extent" : "extents"}
                </span>
              </li>
            ))}
          </ul>
          {summary.value.paths.length > MAX_PATHS_SHOWN && (
            <p className="text-xs text-muted-foreground">
              Showing the first {MAX_PATHS_SHOWN} of {count(summary.value.paths.length)} paths in this view.
            </p>
          )}
        </>
      )}
    </div>
  );
}

// A view that reports nothing needs no section of its own.
function isQuiet(view: AffectedView): boolean {
  const summary = summarizeView(view);
  return (
    view.name.state === "known" &&
    view.unresolvedSegments.state === "known" &&
    view.unresolvedSegments.value.length === 0 &&
    summary.state === "known" &&
    summary.value.paths.length === 0 &&
    summary.value.unreachableReferences === 0
  );
}

function Affected({ member }: { member: string }) {
  const [result, setResult] = useState<Value<AffectedStatus>>(unavailable("Affected-path scan is running"));
  const [lastSuccess, setLastSuccess] = useState<Date | null>(null);
  useEffect(() => {
    const abort = new AbortController();
    void adminClient.getPoolAffected({ member }, { signal: abort.signal, timeoutMs: 60_000 }).then(
      (response) => {
        if (abort.signal.aborted) return;
        if (response.json.length > MAX_AFFECTED_JSON_CHARS) {
          setResult(
            unavailable(
              `The scan answer is ${count(response.json.length)} characters, which is too large to display. Use a smaller scope once the admin API supports paging.`,
            ),
          );
          return;
        }
        const parsed = parseAffectedStatus(response.json, member);
        setResult(parsed);
        if (parsed.state === "known") setLastSuccess(new Date());
      },
      (error: unknown) => {
        if (!abort.signal.aborted) setResult(unavailable(rpcFailure(error).reason));
      },
    );
    return () => abort.abort();
  }, [member]);

  const views = result.state === "known" && result.value.views.state === "known" ? result.value.views.value : [];
  const quiet = views.filter(isQuiet);
  return (
    <section className="card-surface space-y-3 rounded-lg p-5" aria-live="polite">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h2 className="font-medium">Affected paths on {member}</h2>
        {lastSuccess && <p className="text-xs text-muted-foreground">Scan received {lastSuccess.toLocaleString()}</p>}
      </div>
      {result.state === "unavailable" ? (
        <p className="text-sm">
          <Fact value={result} />
        </p>
      ) : (
        <>
          <dl className="text-sm">
            <Row label="Scan complete" value={result.value.complete}>
              {yesNo}
            </Row>
            <Row label="Reported path total across views" value={result.value.pathCount}>
              {count}
            </Row>
          </dl>
          <p className="text-xs text-muted-foreground">
            Paths are counted once per metadata view, so a live path and its checkpoint path count separately. Unlinked
            or unreachable inodes have no paths.
          </p>
          {result.value.errors.state === "unavailable" ? (
            <Fact value={result.value.errors} />
          ) : (
            result.value.errors.value.map((error, i) => (
              <p className="text-sm text-destructive" key={i}>
                {error}
              </p>
            ))
          )}
          {result.value.views.state === "unavailable" ? (
            <Fact value={result.value.views} />
          ) : (
            views.filter((view) => !isQuiet(view)).map((view, i) => <ViewDetails key={i} view={view} />)
          )}
          {quiet.length > 0 && (
            <details className="border-t border-border pt-3 text-sm">
              <summary className="cursor-pointer text-muted hover:text-foreground">
                {quiet.length} {quiet.length === 1 ? "view reports" : "views report"} no affected paths
              </summary>
              <ul className="space-y-1 pt-2 font-mono text-xs text-muted-foreground">
                {quiet.map((view, i) => (
                  <li key={i} className="break-all">
                    <Fact value={view.name} />
                  </li>
                ))}
              </ul>
            </details>
          )}
        </>
      )}
    </section>
  );
}

const capabilities = [
  ["Placement policy", "Policy status and validated edits are not exposed by the current admin API."],
  [
    "Disk profiles and filesystem identity",
    "Declared profiles and verified readiness are not exposed by the current admin API.",
  ],
  [
    "Buffered, locally durable and HDD-published bytes",
    "The current admin API does not expose durability receipts or writeback watermarks.",
  ],
  ["NVMe backlog and reserve", "Journal occupancy and reserve are not exposed by the current admin API."],
  ["NVMe read retention", "Pin coverage and reserved read-copy status are not exposed by the current admin API."],
  ["Per-device write activity", "The current admin API provides no per-device software byte counters."],
] as const;

export function StoragePage() {
  useTitle("Storage");
  const { result, loading, lastSuccess, refresh } = useStoragePool();
  const { activity, lastSuccess: statsReceived } = useStorageActivity();
  const [scan, setScan] = useState<{ member: string; sequence: number } | null>(null);
  const pool = result.state === "pool" ? result.pool : null;
  return (
    <div className="max-w-[1400px] space-y-6 p-6">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <h1 className="text-lg font-semibold">Storage</h1>
          <p className="mt-1 text-sm text-muted-foreground" aria-live="polite">
            {lastSuccess ? `Pool read ${lastSuccess.toLocaleString()}` : "No successful pool response yet"}. Read-only;
            refreshes on request.
          </p>
          {pool && (
            <p className="mt-1 break-all font-mono text-xs text-muted-foreground">
              Pool <Fact value={pool.id} />
            </p>
          )}
        </div>
        <Button variant="outline" disabled={loading} onClick={() => (setScan(null), refresh())}>
          <span className="flex items-center gap-2">
            <RefreshCw size={14} className={loading ? "animate-spin" : ""} />
            Refresh pool
          </span>
        </Button>
      </div>

      {result.state === "no-pool" ? (
        <section className="card-surface rounded-lg p-5 text-sm">
          No permanent pool configured. The admin service explicitly reported this state.
        </section>
      ) : result.state === "unavailable" ? (
        <section className="card-surface rounded-lg p-5 text-sm" aria-live="polite">
          <Fact value={result} />
        </section>
      ) : (
        <dl className="grid grid-cols-2 gap-4 lg:grid-cols-4">
          <Stat label="Mapped objects" value={result.pool.mappedObjects}>
            {count}
          </Stat>
          <Stat label="Pending operations" value={result.pool.pendingOperations}>
            {count}
          </Stat>
          <Stat label="Unreadable segment records" value={result.pool.unreadableObjects}>
            {count}
          </Stat>
          <Stat label="Quarantined leftovers" value={result.pool.quarantinedLeftovers}>
            {count}
          </Stat>
        </dl>
      )}

      {pool && (
        <>
          <div className="space-y-3">
            <h2 className={sectionTitle}>Members</h2>
            {pool.members.state === "unavailable" ? (
              <Fact value={pool.members} />
            ) : pool.members.value.length === 0 ? (
              <p className="text-sm">The server reported no pool members.</p>
            ) : (
              <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
                {pool.members.value.map((member, i) => (
                  <Member
                    key={i}
                    member={member}
                    onScan={(id) => setScan((current) => ({ member: id, sequence: (current?.sequence ?? 0) + 1 }))}
                  />
                ))}
              </div>
            )}
            {pool.unattributedFaults.state === "unavailable" ? (
              <p className="text-sm">
                Segment faults outside the listed members: <Fact value={pool.unattributedFaults} />
              </p>
            ) : (
              pool.unattributedFaults.value.length > 0 && (
                <section className="card-surface space-y-2 rounded-lg p-5 text-sm">
                  <h3 className="font-medium text-destructive">
                    {count(pool.unattributedFaults.value.length)} unreadable segment{" "}
                    {pool.unattributedFaults.value.length === 1 ? "record" : "records"} outside the listed members
                  </h3>
                  <FaultList faults={pool.unattributedFaults.value} label="Unattributed segment faults" />
                </section>
              )
            )}
            <p className="text-xs text-muted-foreground">
              Charged usage includes superseded bodies awaiting cleanup. Reserved uploads are not yet published.
              Allocatable accounts for the configured quota, reservations and the local free-space reserve.
            </p>
          </div>
          {scan && <Affected key={scan.sequence} member={scan.member} />}
        </>
      )}

      <div className="space-y-3">
        <h2 className={sectionTitle}>Filesystem activity</h2>
        {activity.state === "unavailable" ? (
          <section className="card-surface rounded-lg p-5 text-sm">
            <Fact value={activity} />
          </section>
        ) : (
          <dl className="grid grid-cols-2 gap-4 lg:grid-cols-4">
            <Stat label="Logical bytes written" value={activity.value.logicalBytesWritten}>
              {(bytes) => <Bytes value={bytes} />}
            </Stat>
            <Stat label="Logical bytes read" value={activity.value.logicalBytesRead}>
              {(bytes) => <Bytes value={bytes} />}
            </Stat>
            <Stat label="Logical storage used" value={activity.value.logicalUsedBytes}>
              {(bytes) => <Bytes value={bytes} />}
            </Stat>
            <Stat label="Inodes used" value={activity.value.usedInodes}>
              {count}
            </Stat>
          </dl>
        )}
        <p className="text-xs text-muted-foreground">
          Cumulative logical filesystem counters from StreamStats. These are not per-device writes or durability
          receipts. {statsReceived && `Last sample received ${statsReceived.toLocaleString()}.`}
        </p>
      </div>

      <section className="card-surface space-y-4 rounded-lg p-5">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <h2 className={sectionTitle}>Placement</h2>
          <span className="flex items-center gap-1.5 rounded-full bg-accent px-2 py-0.5 text-xs text-muted">
            <Lock size={12} />
            Strict placement is disabled
          </span>
        </div>
        <p className="text-sm text-muted">
          Buffered writes are accepted without durability. Locally durable writes are held in the NVMe journal.
          Published writes require HDD payload and catalog publication. This page cannot assert those states from the
          available APIs.
        </p>
        <dl className="grid grid-cols-1 gap-x-8 gap-y-3 text-sm md:grid-cols-2">
          {capabilities.map(([label, reason]) => (
            <div key={label}>
              <dt className="font-medium">{label}</dt>
              <dd className="text-xs text-muted-foreground">Unavailable · {reason}</dd>
            </div>
          ))}
        </dl>
        <div className="flex flex-wrap items-center gap-3 border-t border-border pt-4">
          <Button variant="outline" disabled aria-describedby="storage-edit-reason">
            Configure placement
          </Button>
          <Button variant="outline" disabled aria-describedby="storage-edit-reason">
            Move or evacuate files
          </Button>
          <p id="storage-edit-reason" className="text-xs text-muted-foreground">
            Disabled: validated policy edits and placement-safe whole-file movement are not available in the current
            backend.
          </p>
        </div>
      </section>
    </div>
  );
}
