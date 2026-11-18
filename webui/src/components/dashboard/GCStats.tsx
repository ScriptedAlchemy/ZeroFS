import { binaryBytesParts } from "../../lib/format";
import type { StatsSnapshot } from "../../lib/grpc/gen/admin_pb";

const bytes = (n: bigint) => binaryBytesParts(n).short;
const tiers = ["None", "Base", "Drain", "Fast"];

function Rows({ items }: { items: [string, string | bigint][] }) {
  return (
    <dl>
      {items.map(([label, value]) => (
        <div key={label} className="flex justify-between gap-2 text-sm py-1">
          <dt className="text-muted">{label}</dt>
          <dd className="font-mono tabular-nums text-right">{value.toLocaleString()}</dd>
        </div>
      ))}
    </dl>
  );
}

export function GCStats({ snapshot }: { snapshot: StatsSnapshot }) {
  const seg = snapshot.segmentGc;
  return (
    <div className="card-surface rounded-lg p-5">
      <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground mb-4">Garbage Collection</p>
      <Rows items={[
        ["GC Runs", snapshot.gcRuns],
        ["Extents Deleted", snapshot.gcExtentsDeleted],
        ["Tombstones Created", snapshot.tombstonesCreated],
        ["Tombstones Processed", snapshot.tombstonesProcessed],
      ]} />
      <h2 className="mt-5 mb-2 font-medium text-sm">Segment reclaim</h2>
      {!seg?.hasRun ? (
        <p className="text-sm text-muted">{seg ? "No reclaim pass yet." : "Segment GC status unavailable."}</p>
      ) : (
        <>
          <p className="mb-2 text-sm">Cadence: {tiers[seg.tier] ?? `Unknown (${seg.tier})`}{seg.reason && ` · ${seg.reason}`}</p>
          {seg.pinned && <p className="mb-2 text-sm text-amber-400">A checkpoint is holding reclaim and compaction.</p>}
          <Rows items={[
            ["Segments at last scan", seg.segmentCount],
            ["Appended", bytes(seg.appendedBytes)],
            ["Live", bytes(seg.liveBytes)],
            ["Reclaimable", bytes(seg.reclaimableBytes)],
            ["Awaiting deletion", `${seg.awaitingDelete.toLocaleString()} (${bytes(seg.awaitingDeleteBytes)})`],
            ["Unflushed now", bytes(seg.unflushedBytes)],
            ["Candidate backlog", seg.candidateBacklog],
            ["Deferred chains", seg.chainsDeferred],
            ["Deleted last pass", `${seg.lastDeleted.toLocaleString()} (${bytes(seg.lastDeletedBytes)})`],
            ["Frames relocated", seg.lastFramesRelocated],
          ]} />
        </>
      )}
    </div>
  );
}
