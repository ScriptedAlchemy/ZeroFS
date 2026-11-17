import { useState, useEffect } from "react";
import { adminClient } from "../lib/grpc/client";
import type { StatsSnapshot } from "../lib/grpc/gen/admin_pb";
import { known, unavailable } from "../lib/storage-status";
import type { Value } from "../lib/storage-status";

const HISTORY_SIZE = 120;
const RECONNECT_DELAY = 2000;

export function useStatsStream(intervalMs = 1000) {
  const [history, setHistory] = useState<StatsSnapshot[]>([]);
  const [latest, setLatest] = useState<Value<StatsSnapshot>>(unavailable("Stats stream has not supplied a sample"));
  const [receivedAt, setReceivedAt] = useState<Date | null>(null);

  useEffect(() => {
    const abort = new AbortController();

    const connect = async () => {
      while (!abort.signal.aborted) {
        try {
          let first = true;
          for await (const snap of adminClient.streamStats({ intervalMs }, { signal: abort.signal })) {
            if (abort.signal.aborted) return;
            setLatest(known(snap));
            setReceivedAt(new Date());
            if (first) {
              setHistory([snap]);
              first = false;
            } else {
              setHistory((prev) => [...prev.slice(1 - HISTORY_SIZE), snap]);
            }
          }
          if (!abort.signal.aborted) setLatest(unavailable("Stats stream ended"));
        } catch (e) {
          if (abort.signal.aborted) return;
          const failure = e instanceof Error ? e : new Error(String(e));
          setLatest(unavailable(failure.message));
        }
        if (!abort.signal.aborted) {
          await new Promise((r) => setTimeout(r, RECONNECT_DELAY));
        }
      }
    };

    connect();
    return () => abort.abort();
  }, [intervalMs]);

  return { history, latest, receivedAt };
}
