import { useState, useEffect } from "react";
import { adminClient } from "../lib/grpc/client";
import type { StatsSnapshot } from "../lib/grpc/gen/admin_pb";
import { known, unavailable } from "../lib/storage-status";
import type { Value } from "../lib/storage-status";

const HISTORY_SIZE = 120;
const RECONNECT_DELAY = 2000;

export function useStatsStream(intervalMs = 1000) {
  const [snapshot, setSnapshot] = useState<StatsSnapshot | null>(null);
  const [history, setHistory] = useState<StatsSnapshot[]>([]);
  const [error, setError] = useState<Error | null>(null);
  // Unlike snapshot, this stays unavailable from a failure or a clean end until the next sample arrives.
  const [latest, setLatest] = useState<Value<StatsSnapshot>>(unavailable("Stats stream has not supplied a sample"));
  const [receivedAt, setReceivedAt] = useState<Date | null>(null);

  useEffect(() => {
    const abort = new AbortController();
    let seeded = false;

    const connect = async () => {
      while (!abort.signal.aborted) {
        try {
          setError(null);
          for await (const snap of adminClient.streamStats({ intervalMs }, { signal: abort.signal })) {
            setSnapshot(snap);
            setLatest(known(snap));
            setReceivedAt(new Date());
            setHistory((prev) => {
              if (!seeded) {
                seeded = true;
                return Array.from({ length: HISTORY_SIZE }, () => snap);
              }
              const next = [...prev, snap];
              if (next.length > HISTORY_SIZE) next.shift();
              return next;
            });
          }
          if (!abort.signal.aborted) setLatest(unavailable("Stats stream ended"));
        } catch (e) {
          if (abort.signal.aborted) return;
          const failure = e instanceof Error ? e : new Error(String(e));
          setError(failure);
          setLatest(unavailable(failure.message));
        }
        if (!abort.signal.aborted) {
          seeded = false;
          await new Promise((r) => setTimeout(r, RECONNECT_DELAY));
        }
      }
    };

    connect();
    return () => abort.abort();
  }, [intervalMs]);

  return { snapshot, history, error, latest, receivedAt };
}
