import { useEffect, useState } from "react";
import { HardDrive, RefreshCw } from "lucide-react";
import { useTitle } from "../hooks/use-title";
import { useStorageActivity, useStoragePool } from "../hooks/use-storage";
import { adminClient } from "../lib/grpc/client";
import { formatBinaryBytes } from "../lib/format";
import { known, parseAffectedStatus, rpcFailure, unavailable } from "../lib/storage-status";
import type { AffectedStatus, AffectedView, MemberStatus, Value } from "../lib/storage-status";
import { Button } from "../components/ui/Button";

function Fact<T>({ value, format = String }: { value: Value<T>; format?: (value: T) => string }) {
  return value.state === "known" ? (
    <span className="break-all">{format(value.value)}</span>
  ) : (
    <span className="text-muted-foreground" title={value.reason}>
      unavailable. <span className="text-xs">{value.reason}</span>
    </span>
  );
}

function Row<T>({ label, value, format }: { label: string; value: Value<T>; format?: (value: T) => string }) {
  return (
    <div className="flex flex-wrap justify-between gap-x-5 gap-y-1">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="font-mono tabular-nums text-right">
        <Fact value={value} format={format} />
      </dd>
    </div>
  );
}

const yesNo = (value: boolean) => (value ? "yes" : "no");
const observation = (value: string | null) => (value === null ? "none reported" : value);

function Member({ member, onScan }: { member: MemberStatus; onScan: (id: string) => void }) {
  const id = member.id;
  return (
    <section className="card-surface rounded-lg p-5 space-y-4">
      <div className="flex items-center justify-between gap-3">
        <h3 className="font-medium flex items-center gap-2">
          <HardDrive size={17} />
          <Fact value={id} />
        </h3>
        <Button
          variant="primary"
          disabled={id.state !== "known"}
          onClick={() => {
            if (id.state === "known") onScan(id.value);
          }}
        >
          Inspect affected paths
        </Button>
      </div>
      <dl className="space-y-2 text-sm">
        <Row label="Configured capacity" value={member.capacity} format={formatBinaryBytes} />
        <Row label="Charged usage" value={member.used} format={formatBinaryBytes} />
        <Row label="Reserved uploads" value={member.reserved} format={formatBinaryBytes} />
        <Row label="Allocatable" value={member.available} format={formatBinaryBytes} />
        <Row label="Objects" value={member.objects} />
        <Row label="Pending uploads" value={member.pendingUploads} />
        <Row label="Cleanup debts" value={member.cleanupDebts} />
        <Row label="Unreadable objects" value={member.unreadableObjects} />
        <Row label="Evacuation" value={member.evacuation} />
        <Row label="Quarantine" value={member.quarantine} format={observation} />
        <Row label="Monitoring error" value={member.monitoringError} format={observation} />
        <Row label="Warning" value={member.warning} format={observation} />
        <Row label="Evacuation error" value={member.evacuationError} format={observation} />
        <Row label="Health observation" value={member.observedMs} format={(ms) => new Date(ms).toLocaleString()} />
      </dl>
      <details className="text-sm">
        <summary className="cursor-pointer text-muted-foreground">Backend removal observations</summary>
        <dl className="space-y-2 pt-3">
          <Row label="Safe to remove" value={member.safeToRemove} format={yesNo} />
          <Row label="Safe to pull with known loss" value={member.safeToPullWithKnownLoss} format={yesNo} />
          <Row label="Readable data evacuated" value={member.readableEvacuated} format={yesNo} />
        </dl>
        <p className="text-xs text-muted-foreground mt-3">
          These are existing pool observations. They do not establish whole-file relocation or metadata survival after
          device loss.
        </p>
      </details>
    </section>
  );
}

function AffectedViewDetails({ view }: { view: AffectedView }) {
  return (
    <div className="space-y-2 border-t border-border pt-3">
      <h4 className="font-medium">
        <Fact value={view.name} />
      </h4>
      <p className="text-xs text-muted-foreground">
        Unresolved segments:{" "}
        <Fact value={view.unresolvedSegments} format={(items) => (items.length ? items.join(", ") : "none reported")} />
      </p>
      {view.references.state === "unavailable" ? (
        <Fact value={view.references} />
      ) : (
        view.references.value.map((reference, index) => (
          <div key={index} className="text-sm space-y-1">
            <p className="text-xs text-muted-foreground">
              Inode <Fact value={reference.inode} />, extent <Fact value={reference.extent} />, segment{" "}
              <Fact value={reference.segment} />
            </p>
            {reference.paths.state === "unavailable" ? (
              <Fact value={reference.paths} />
            ) : reference.paths.value.length === 0 ? (
              <p className="text-muted-foreground">No reachable pathname reported for this reference.</p>
            ) : (
              reference.paths.value.map((path) => (
                <details key={path.hex} className="font-mono break-all">
                  <summary className="cursor-pointer">
                    {path.display}
                    {!path.utf8 && <span className="text-xs text-muted-foreground ml-2">non-UTF-8; escaped bytes</span>}
                  </summary>
                  <p className="text-xs text-muted-foreground pl-4">Path bytes (hex): {path.hex}</p>
                </details>
              ))
            )}
          </div>
        ))
      )}
    </div>
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

  return (
    <section className="card-surface rounded-lg p-5 space-y-3" aria-live="polite">
      <h2 className="font-medium">Affected paths · {member}</h2>
      {lastSuccess && <p className="text-xs text-muted-foreground">Scan received {lastSuccess.toLocaleString()}</p>}
      {result.state === "unavailable" ? (
        <Fact value={result} />
      ) : (
        <>
          <dl className="text-sm space-y-2">
            <Row label="Scan complete" value={result.value.complete} format={yesNo} />
            <Row label="Reported path total across views" value={result.value.pathCount} />
            <Row label="Coverage" value={result.value.coverage} />
          </dl>
          <p className="text-xs text-muted-foreground">
            Paths are counted once per metadata view; a live path and its checkpoint path count separately. Unlinked or
            unreachable inodes have no paths.
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
            result.value.views.value.map((view, i) => <AffectedViewDetails key={i} view={view} />)
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
    "Buffered / locally durable / HDD-published bytes",
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
    <div className="p-6 space-y-5 max-w-[1400px]">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold">Storage</h1>
          <p className="text-sm text-muted-foreground mt-1">Read-only observations from the existing admin service.</p>
        </div>
        <Button
          variant="primary"
          disabled={loading}
          onClick={() => {
            setScan(null);
            refresh();
          }}
        >
          <span className="flex items-center gap-2">
            <RefreshCw size={14} className={loading ? "animate-spin" : ""} />
            Refresh pool
          </span>
        </Button>
      </div>
      <section className="card-surface rounded-lg p-5 space-y-3" aria-live="polite">
        <h2 className="font-medium">Pool</h2>
        <p className="text-xs text-muted-foreground">
          {lastSuccess
            ? `Last successful response: ${lastSuccess.toLocaleString()}`
            : "No successful pool response yet"}
          . Pool observations refresh on request.
        </p>
        {result.state === "no-pool" ? (
          <p>No permanent pool configured. The admin service explicitly reported this state.</p>
        ) : result.state === "unavailable" ? (
          <Fact value={result} />
        ) : (
          <dl className="space-y-2 text-sm">
            <Row label="Pool ID" value={result.pool.id} />
            <Row label="Mapped objects" value={result.pool.mappedObjects} />
            <Row label="Pending operations" value={result.pool.pendingOperations} />
            <Row label="Unreadable segment records" value={result.pool.unreadableObjects} />
            <Row label="Quarantined leftovers" value={result.pool.quarantinedLeftovers} />
          </dl>
        )}
      </section>
      {pool && (
        <>
          <div>
            <h2 className="font-medium mb-3">Members</h2>
            <p className="text-xs text-muted-foreground mb-3">
              Charged usage includes superseded bodies awaiting cleanup; reserved bytes include unpublished uploads.
              Allocatable bytes account for configured quota, reservations, and the local free-space reserve.
            </p>
            {pool.members.state === "unavailable" ? (
              <Fact value={pool.members} />
            ) : pool.members.value.length === 0 ? (
              <p className="text-sm">The server reported no pool members.</p>
            ) : (
              <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
                {pool.members.value.map((member, i) => (
                  <Member
                    key={i}
                    member={member}
                    onScan={(id) => setScan((current) => ({ member: id, sequence: (current?.sequence ?? 0) + 1 }))}
                  />
                ))}
              </div>
            )}
          </div>
          {scan && <Affected key={scan.sequence} member={scan.member} />}
        </>
      )}
      <section className="card-surface rounded-lg p-5 space-y-3">
        <h2 className="font-medium">Filesystem activity</h2>
        <p className="text-xs text-muted-foreground">
          Cumulative logical filesystem counters from StreamStats. These are not per-device writes or durability
          receipts. {statsReceived && `Last sample received ${statsReceived.toLocaleString()}.`}
        </p>
        {activity.state === "unavailable" ? (
          <Fact value={activity} />
        ) : (
          <>
            <p className="text-xs text-muted-foreground">
              StreamStats sends these counters as plain protobuf numbers, so a counter the server omits arrives as 0. A
              0 here can mean zero or not reported.
            </p>
            <dl className="text-sm space-y-2">
              <Row
                label="Logical bytes written"
                value={known(activity.value.logicalBytesWritten)}
                format={formatBinaryBytes}
              />
              <Row
                label="Logical bytes read"
                value={known(activity.value.logicalBytesRead)}
                format={formatBinaryBytes}
              />
              <Row
                label="Logical storage used"
                value={known(activity.value.logicalUsedBytes)}
                format={formatBinaryBytes}
              />
              <Row label="Inodes used" value={known(activity.value.usedInodes)} />
            </dl>
          </>
        )}
      </section>
      <section className="card-surface rounded-lg p-5 space-y-4">
        <h2 className="font-medium">Placement capabilities</h2>
        <dl className="space-y-3 text-sm">
          {capabilities.map(([label, reason]) => (
            <div key={label}>
              <dt className="font-medium">{label}</dt>
              <dd className="text-muted-foreground">unavailable. {reason}</dd>
            </div>
          ))}
        </dl>
        <p className="text-sm text-muted-foreground">
          Strict placement is disabled. Buffered writes are accepted without durability; locally durable writes are held
          in the NVMe journal; published writes require HDD payload and catalog publication. This page cannot assert
          those states from the available APIs.
        </p>
        <div className="flex flex-wrap gap-3">
          <Button variant="primary" disabled aria-describedby="storage-edit-reason">
            Configure placement
          </Button>
          <Button variant="primary" disabled aria-describedby="storage-edit-reason">
            Move or evacuate files
          </Button>
        </div>
        <p id="storage-edit-reason" className="text-xs text-muted-foreground">
          Disabled: validated policy edits and placement-safe whole-file movement are not available in the current
          backend.
        </p>
      </section>
    </div>
  );
}
