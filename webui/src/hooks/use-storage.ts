import { useCallback, useEffect, useState } from "react";
import { adminClient } from "../lib/grpc/client";
import { loadPoolStatus, known, unavailable } from "../lib/storage-status";
import type { PoolResult, Value } from "../lib/storage-status";
import { useStatsStream } from "./use-stats";

export interface StorageActivity {
  logicalBytesWritten: Value<bigint>;
  logicalBytesRead: Value<bigint>;
  logicalUsedBytes: Value<bigint>;
  usedInodes: Value<bigint>;
}

// These StatsSnapshot counters carry presence: the server omits one it cannot state.
const counter = (value: bigint | undefined, name: string): Value<bigint> =>
  value === undefined ? unavailable(`${name} was not reported by the server`) : known(value);

export function useStoragePool() {
  const [result, setResult] = useState<PoolResult>(unavailable("Pool status has not been received"));
  const [lastSuccess, setLastSuccess] = useState<Date | null>(null);
  const [loading, setLoading] = useState(true);
  const [generation, setGeneration] = useState(0);
  const refresh = useCallback(() => {
    setLoading(true);
    setResult(unavailable("Refreshing pool status"));
    setGeneration((value) => value + 1);
  }, []);

  useEffect(() => {
    const abort = new AbortController();
    void loadPoolStatus(() => adminClient.getPoolStatus({}, { signal: abort.signal, timeoutMs: 30_000 })).then(
      (next) => {
        if (abort.signal.aborted) return;
        setResult(next);
        setLoading(false);
        if (next.state === "pool" || next.state === "no-pool") setLastSuccess(new Date());
      },
    );
    return () => abort.abort();
  }, [generation]);

  return { result, loading, lastSuccess, refresh };
}

export function useStorageActivity(): { activity: Value<StorageActivity>; lastSuccess: Date | null } {
  const { latest, receivedAt } = useStatsStream(1000);
  const activity =
    latest.state === "unavailable"
      ? latest
      : known({
          logicalBytesWritten: counter(latest.value.bytesWritten, "bytes_written"),
          logicalBytesRead: counter(latest.value.bytesRead, "bytes_read"),
          logicalUsedBytes: counter(latest.value.usedBytes, "used_bytes"),
          usedInodes: counter(latest.value.usedInodes, "used_inodes"),
        });
  return { activity, lastSuccess: receivedAt };
}
