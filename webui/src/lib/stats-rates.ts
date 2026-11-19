import type { StatsSnapshot } from "./grpc/gen/admin_pb";

type RateSample = Pick<StatsSnapshot, "timestamp" | "bytesRead" | "bytesWritten" | "readOperations" | "writeOperations">;
export type RateMetric = "throughput" | "iops";

export function statsRates(history: RateSample[], metric: RateMetric) {
  const points: { time: number; read: number | null; write: number | null }[] = [];
  const read = metric === "throughput" ? "bytesRead" : "readOperations";
  const write = metric === "throughput" ? "bytesWritten" : "writeOperations";
  let previousTime: number | undefined;

  for (const [i, sample] of history.entries()) {
    const { timestamp } = sample;
    if (!timestamp) {
      previousTime = undefined;
      continue;
    }
    const time = Number(timestamp.seconds) * 1000 + timestamp.nanos / 1e6;
    if (points.length && time <= points[points.length - 1].time) {
      points.length = 0;
      previousTime = undefined;
    }
    const seconds = previousTime === undefined ? 0 : (time - previousTime) / 1000;
    const previous = history[i - 1];
    points.push({
      time,
      read: seconds > 0 && sample[read] >= previous[read] ? Number(sample[read] - previous[read]) / seconds : null,
      write: seconds > 0 && sample[write] >= previous[write] ? Number(sample[write] - previous[write]) / seconds : null,
    });
    previousTime = time;
  }
  return points;
}
