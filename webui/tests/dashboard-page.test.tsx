import { create } from "@bufbuild/protobuf";
import { act, cleanup, fireEvent, render, renderHook, screen } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { GCStats } from "../src/components/dashboard/GCStats";
import { FileAccessTracer } from "../src/components/dashboard/FileAccessTracer";
import { useStatsStream } from "../src/hooks/use-stats";
import { FileAccessEventSchema, StatsSnapshotSchema } from "../src/lib/grpc/gen/admin_pb";
import { statsRates } from "../src/lib/stats-rates";
import { DashboardPage } from "../src/pages/dashboard";

const client = vi.hoisted(() => ({ streamStats: vi.fn(), watchFileAccess: vi.fn() }));
vi.mock("../src/lib/grpc/client", () => ({ adminClient: client }));

function feed() {
  let controller!: ReadableStreamDefaultController;
  const stream = new ReadableStream({ start(c) { controller = c; } });
  return { stream, controller };
}

function sample(seconds: bigint, n: bigint, nanos = 0) {
  return create(StatsSnapshotSchema, {
    timestamp: { seconds, nanos }, totalOperations: n,
    bytesRead: n * 2n, bytesWritten: n, readOperations: n,
  });
}

beforeEach(() => {
  vi.useFakeTimers();
  client.streamStats.mockReturnValue(feed().stream);
  client.watchFileAccess.mockReturnValue(feed().stream);
});
afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.resetAllMocks();
});

it.each(["end", "error"])("withdraws old stats on stream %s until a new sample arrives", async (ending) => {
  const first = feed();
  const second = feed();
  client.streamStats.mockReturnValueOnce(first.stream).mockReturnValueOnce(second.stream);
  render(<DashboardPage />);
  await act(async () => first.controller.enqueue(sample(1n, 42n)));
  expect(screen.getByText("Sample received")).toBeTruthy();
  expect(screen.getByText("42")).toBeTruthy();
  expect(screen.getAllByText("Waiting for two timestamped samples.")).toHaveLength(2);
  await act(async () => {
    if (ending === "end") first.controller.close();
    else first.controller.error(new Error("stats unavailable"));
  });
  const message = ending === "end" ? "Stats stream ended" : "stats unavailable";
  expect(screen.getByText(message)).toBeTruthy();
  expect(screen.getByText(/Last sample received/)).toBeTruthy();
  expect(screen.queryByText("42")).toBeNull();
  expect(screen.getByText("File Access Trace")).toBeTruthy();
  await act(async () => vi.advanceTimersByTimeAsync(2000));
  expect(client.streamStats).toHaveBeenCalledTimes(2);
  expect(screen.getByText(message)).toBeTruthy();
  await act(async () => second.controller.enqueue(sample(20n, 71n)));
  expect(screen.getByText("71")).toBeTruthy();
  expect(screen.queryByText(message)).toBeNull();
});

it("keeps only observed stats and starts a new history after reconnect", async () => {
  const first = feed();
  const second = feed();
  client.streamStats.mockReturnValueOnce(first.stream).mockReturnValueOnce(second.stream);
  const { result } = renderHook(() => useStatsStream());
  await act(async () => {
    first.controller.enqueue(sample(1n, 1n));
    first.controller.enqueue(sample(2n, 2n));
  });
  expect(result.current.history.map((s) => s.totalOperations)).toEqual([1n, 2n]);
  await act(async () => first.controller.close());
  await act(async () => vi.advanceTimersByTimeAsync(2000));
  await act(async () => second.controller.enqueue(sample(30n, 9n)));
  expect(result.current.history.map((s) => s.totalOperations)).toEqual([9n]);
});

it.each(["end", "error"])("reports trace %s and reconnects with prior events visible", async (ending) => {
  const first = feed();
  const second = feed();
  client.watchFileAccess.mockReturnValueOnce(first.stream).mockReturnValueOnce(second.stream);
  render(<FileAccessTracer />);
  await act(async () => first.controller.enqueue(create(FileAccessEventSchema, { path: "/before" })));
  await act(async () => {
    if (ending === "end") first.controller.close();
    else first.controller.error(new Error("trace unavailable"));
  });
  expect(screen.getByRole("status").textContent).toContain(ending === "end" ? "ended" : "trace unavailable");
  expect(screen.queryByText("Waiting for events...")).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "Reconnect trace" }));
  await act(async () => second.controller.enqueue(create(FileAccessEventSchema, { path: "/after" })));
  expect(screen.queryByRole("status")).toBeNull();
  expect(screen.getByText("/before")).toBeTruthy();
  expect(screen.getByText("/after")).toBeTruthy();
});

it("computes chart rates from timestamped intervals and preserves measured zero", () => {
  const history = [sample(20n, 5n), sample(22n, 10n, 500_000_000)];
  expect(statsRates(history, "throughput")).toEqual([
    { time: 20000, read: null, write: null }, { time: 22500, read: 4, write: 2 },
  ]);
  expect(statsRates(history, "iops")[1]).toEqual({ time: 22500, read: 2, write: 0 });
});

it("skips rates across missing timestamps, backward clocks and counter resets", () => {
  const unknown = create(StatsSnapshotSchema);
  const history = [sample(10n, 10n), sample(11n, 20n), unknown, sample(40n, 50n), sample(41n, 2n)];
  expect(statsRates(history, "throughput").slice(-2)).toEqual([
    { time: 40000, read: null, write: null }, { time: 41000, read: null, write: null },
  ]);
  expect(statsRates([...history, sample(1n, 10n), sample(1n, 20n), sample(2n, 30n)], "throughput")).toEqual([
    { time: 1000, read: null, write: null }, { time: 2000, read: 20, write: 10 },
  ]);
});

it("distinguishes unreported, pending and observed segment GC status", () => {
  const { rerender } = render(<GCStats snapshot={create(StatsSnapshotSchema)} />);
  expect(screen.getByText("Segment GC status unavailable.")).toBeTruthy();
  rerender(<GCStats snapshot={create(StatsSnapshotSchema, { segmentGc: {} })} />);
  expect(screen.getByText("No reclaim pass yet.")).toBeTruthy();
  expect(screen.queryByText("Reclaimable")).toBeNull();
  const snapshot = create(StatsSnapshotSchema, { segmentGc: {
    hasRun: true, tier: 2, reason: "candidate backlog", pinned: true,
    segmentCount: 7n, appendedBytes: 4096n, liveBytes: 2048n, reclaimableBytes: 2048n,
  } });
  rerender(<GCStats snapshot={snapshot} />);
  expect(screen.getByText("Cadence: Drain · candidate backlog")).toBeTruthy();
  expect(screen.getByText(/checkpoint is holding/)).toBeTruthy();
  expect(screen.getByText("Reclaimable").nextElementSibling?.textContent).toBe("2.0 KiB");
  expect(screen.getByText("Candidate backlog").nextElementSibling?.textContent).toBe("0");
});
