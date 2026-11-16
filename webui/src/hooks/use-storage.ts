import { useCallback, useEffect, useState } from "react";
import { adminClient } from "../lib/grpc/client";
import { loadPoolStatus, known, rpcFailure, unavailable } from "../lib/storage-status";
import type { PoolResult, Value } from "../lib/storage-status";

export interface StorageActivity {
  logicalBytesWritten: bigint;
  logicalBytesRead: bigint;
  logicalUsedBytes: bigint;
  usedInodes: bigint;
}

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

export function useStorageActivity() {
  const [activity, setActivity] = useState<Value<StorageActivity>>(
    unavailable("Stats stream has not supplied a sample"),
  );
  const [lastSuccess, setLastSuccess] = useState<Date | null>(null);

  useEffect(() => {
    const abort = new AbortController();
    async function run() {
      while (!abort.signal.aborted) {
        try {
          for await (const sample of adminClient.streamStats({ intervalMs: 1000 }, { signal: abort.signal })) {
            if (abort.signal.aborted) return;
            setActivity(
              known({
                logicalBytesWritten: sample.bytesWritten,
                logicalBytesRead: sample.bytesRead,
                logicalUsedBytes: sample.usedBytes,
                usedInodes: sample.usedInodes,
              }),
            );
            setLastSuccess(new Date());
          }
          if (!abort.signal.aborted) setActivity(unavailable("Stats stream ended"));
        } catch (error) {
          if (abort.signal.aborted) return;
          setActivity(unavailable(rpcFailure(error).reason));
        }
        if (!abort.signal.aborted) {
          await new Promise<void>((resolve) => {
            const timer = setTimeout(done, 2000);
            function done() {
              clearTimeout(timer);
              abort.signal.removeEventListener("abort", done);
              resolve();
            }
            abort.signal.addEventListener("abort", done, { once: true });
          });
        }
      }
    }
    void run();
    return () => abort.abort();
  }, []);

  return { activity, lastSuccess };
}
