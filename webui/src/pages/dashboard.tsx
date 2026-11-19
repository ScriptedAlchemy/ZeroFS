import { Activity, Database, HardDrive, Zap, Weight } from "lucide-react";
import { useTitle } from "../hooks/use-title";
import { useStatsStream } from "../hooks/use-stats";
import type { StatsSnapshot } from "../lib/grpc/gen/admin_pb";
import { formatSize } from "../lib/format";
import { StatCard } from "../components/dashboard/StatCard";
import { IOChart } from "../components/dashboard/IOChart";
import { OperationCounters } from "../components/dashboard/OperationCounters";
import { GCStats } from "../components/dashboard/GCStats";
import { FileAccessTracer } from "../components/dashboard/FileAccessTracer";

// The server omits a counter it cannot state; show that instead of a 0.
const reported = (value: bigint | undefined, show: (value: bigint) => string) =>
  value === undefined ? "Unavailable" : show(value);

export function DashboardPage() {
  useTitle("Monitor");
  const { latest, history, receivedAt } = useStatsStream(1000);

  return (
    <div className="p-6 space-y-5 max-w-[1400px] overflow-hidden">
      <div className="flex items-center gap-3">
        <h1 className="text-lg font-semibold">Monitor</h1>
        {latest.state === "known" && receivedAt && (
          <span className="text-xs text-muted-foreground">
            Sample received <time dateTime={receivedAt.toISOString()}>{receivedAt.toLocaleString()}</time>
          </span>
        )}
      </div>
      {latest.state === "known" ? (
        <StatsPanels snapshot={latest.value} history={history} />
      ) : (
        <div role="status" className="card-surface rounded-lg p-5 text-sm">
          <p>{latest.reason}</p>
          {receivedAt && <p className="text-muted-foreground">Last sample received {receivedAt.toLocaleString()}.</p>}
        </div>
      )}
      <FileAccessTracer />
    </div>
  );
}

function StatsPanels({ snapshot, history }: { snapshot: StatsSnapshot; history: StatsSnapshot[] }) {
  return (
    <>
      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-5 gap-4">
        <StatCard
          title="Total Operations"
          value={snapshot.totalOperations.toLocaleString()}
          icon={<Zap size={18} strokeWidth={1.5} />}
        />
        <StatCard
          title="Bytes Read"
          value={formatSize(snapshot.bytesRead)}
          icon={<Activity size={18} strokeWidth={1.5} />}
        />
        <StatCard
          title="Bytes Written"
          value={formatSize(snapshot.bytesWritten)}
          icon={<HardDrive size={18} strokeWidth={1.5} />}
        />
        <StatCard
          title="Storage Used"
          value={reported(snapshot.usedBytes, formatSize)}
          icon={<Weight size={18} strokeWidth={1.5} />}
        />
        <StatCard
          title="Inodes Used"
          value={reported(snapshot.usedInodes, (n) => n.toLocaleString())}
          icon={<Database size={18} strokeWidth={1.5} />}
        />
      </div>

      <IOChart history={history} metric="throughput" />
      <IOChart history={history} metric="iops" />

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
        <OperationCounters snapshot={snapshot} />
        <GCStats snapshot={snapshot} />
        <div className="card-surface rounded-lg p-5">
          <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground mb-3">Properties</p>
          <dl className="space-y-2 text-sm">
            <div className="flex justify-between">
              <dt className="text-muted-foreground">Files created</dt>
              <dd className="font-mono tabular-nums">{snapshot.filesCreated.toLocaleString()}</dd>
            </div>
            <div className="flex justify-between">
              <dt className="text-muted-foreground">Files deleted</dt>
              <dd className="font-mono tabular-nums">{snapshot.filesDeleted.toLocaleString()}</dd>
            </div>
            <div className="flex justify-between">
              <dt className="text-muted-foreground">Files renamed</dt>
              <dd className="font-mono tabular-nums">{snapshot.filesRenamed.toLocaleString()}</dd>
            </div>
            <div className="flex justify-between">
              <dt className="text-muted-foreground">Dirs created</dt>
              <dd className="font-mono tabular-nums">{snapshot.directoriesCreated.toLocaleString()}</dd>
            </div>
            <div className="flex justify-between">
              <dt className="text-muted-foreground">Dirs deleted</dt>
              <dd className="font-mono tabular-nums">{snapshot.directoriesDeleted.toLocaleString()}</dd>
            </div>
            <div className="flex justify-between">
              <dt className="text-muted-foreground">Links created</dt>
              <dd className="font-mono tabular-nums">{snapshot.linksCreated.toLocaleString()}</dd>
            </div>
            <div className="flex justify-between">
              <dt className="text-muted-foreground">Tombstones</dt>
              <dd className="font-mono tabular-nums">{snapshot.tombstonesCreated.toLocaleString()} / {snapshot.tombstonesProcessed.toLocaleString()}</dd>
            </div>
          </dl>
        </div>
      </div>
    </>
  );
}
