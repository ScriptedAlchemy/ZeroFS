import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { Code, ConnectError } from "@connectrpc/connect";

// The generated gRPC client is not present when CI runs tests, so the page gets a fake admin client.
const client = vi.hoisted(() => ({
  getPoolStatus: vi.fn(),
  getPoolAffected: vi.fn(),
  streamStats: vi.fn(),
}));
vi.mock("../src/lib/grpc/client", () => ({ adminClient: client }));

import { StoragePage } from "../src/pages/storage";

const member = {
  id: "hdd-a",
  capacity_bytes: 18_000_000_000_000,
  used_bytes: 3_000_000_000_000,
  reserved_bytes: 1024,
  available_bytes: 14_000_000_000_000,
  objects: 3,
  pending_uploads: 1,
  cleanup_debts: 2,
  unreadable_objects: 0,
  evacuation: "idle",
  safe_to_remove: false,
  safe_to_pull_with_known_loss: false,
  readable_evacuated: false,
  health: {
    quarantine: null,
    monitoring_error: null,
    warning: null,
    evacuation_error: null,
    kernel_coverage: "current boot kernel ring; unrecognized device/error forms are not attributed",
    last_sample: { device: "synthetic", failed: false },
    since_ms: null,
    observed_ms: null,
  },
  evacuation_capacity: { sufficient: true, unavailable_members: [] },
};
// The server's clock when it assembled the status: one minute after the observations used below.
const generatedMs = 1_800_000_060_000;
const pool = (members: unknown[], unreadable: unknown[] = [], extra: Record<string, unknown> = {}) =>
  JSON.stringify({
    pool_id: "test-pool",
    generated_ms: generatedMs,
    ...extra,
    members,
    pending_operations: 0,
    mapped_objects: 3,
    unreadable,
    quarantined_leftovers: [],
  });
// A decoded StatsSnapshot. Its usage counters carry presence, so the client sees an omitted one as undefined.
type Sample = { bytesWritten?: bigint; bytesRead?: bigint; usedBytes?: bigint; usedInodes?: bigint };
const sample: Sample = { bytesWritten: 5n * 1024n ** 3n, bytesRead: 0n, usedBytes: 2048n, usedInodes: 0n };

// A stream that stays open until the page aborts it.
function openStream(_request: unknown, options?: { signal?: AbortSignal }): AsyncIterable<Sample> {
  return {
    [Symbol.asyncIterator]: () => ({
      next: () =>
        new Promise((_resolve, reject) =>
          options?.signal?.addEventListener("abort", () => reject(new Error("aborted")), { once: true }),
        ),
    }),
  };
}

function failingStream(error: Error): AsyncIterable<Sample> {
  return { [Symbol.asyncIterator]: () => ({ next: () => Promise.reject(error) }) };
}

function without(record: Record<string, unknown>, ...keys: string[]) {
  return Object.fromEntries(Object.entries(record).filter(([key]) => !keys.includes(key)));
}

function rowValue(label: string): string {
  const term = screen.getByText(label).closest("dt");
  return term?.nextElementSibling?.textContent ?? "";
}

beforeEach(() => {
  client.getPoolStatus.mockResolvedValue({ json: pool([member]) });
  client.getPoolAffected.mockReset();
  client.streamStats.mockImplementation(openStream);
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe("Storage page pool section", () => {
  it("labels capacity rows with binary units and the exact byte count", async () => {
    render(<StoragePage />);
    await screen.findByText("hdd-a");
    expect(rowValue("Configured capacity")).toBe("16.4 TiB (18,000,000,000,000 bytes)");
    expect(rowValue("Charged usage")).toBe("2.7 TiB (3,000,000,000,000 bytes)");
    expect(rowValue("Reserved uploads")).toBe("1.0 KiB (1,024 bytes)");
    expect(document.body.textContent).not.toMatch(/\d TB\b/);
  });

  it("renders a missing member counter as unavailable, never as 0", async () => {
    client.getPoolStatus.mockResolvedValue({ json: pool([without(member, "capacity_bytes", "used_bytes")]) });
    render(<StoragePage />);
    await screen.findByText("hdd-a");
    for (const label of ["Configured capacity", "Charged usage"]) {
      expect(rowValue(label)).toMatch(/^Unavailable/);
      expect(rowValue(label)).not.toMatch(/\b0\b/);
    }
  });

  it("shows the explicit no-pool state only for the server's no-pool answer", async () => {
    client.getPoolStatus.mockRejectedValue(new ConnectError("no permanent pool configured", Code.FailedPrecondition));
    render(<StoragePage />);
    expect(await screen.findByText(/No permanent pool configured/)).toBeTruthy();
    expect(screen.queryByText("Members")).toBeNull();
  });

  it("renders a pool RPC failure as unavailable, not as an empty or destroyed pool", async () => {
    client.getPoolStatus.mockRejectedValue(new ConnectError("connection refused", Code.Unavailable));
    render(<StoragePage />);
    expect(await screen.findByText(/connection refused/)).toBeTruthy();
    expect(screen.queryByText("Members")).toBeNull();
    expect(screen.queryByText(/No permanent pool configured/)).toBeNull();
    expect(screen.queryByText(/reported no pool members/)).toBeNull();
  });
});

describe("Storage page controls", () => {
  it("keeps placement controls disabled and sends no request when they are clicked", async () => {
    render(<StoragePage />);
    await screen.findByText("hdd-a");
    for (const name of ["Configure placement", "Move or evacuate files"]) {
      const button = screen.getByRole("button", { name }) as HTMLButtonElement;
      expect(button.disabled).toBe(true);
      expect(button.getAttribute("aria-describedby")).toBe("storage-edit-reason");
      fireEvent.click(button);
    }
    expect(screen.getByText(/^Disabled: validated policy edits/)).toBeTruthy();
    expect(client.getPoolAffected).not.toHaveBeenCalled();
  });

  it("disables the affected-path scan for a member whose id is missing", async () => {
    client.getPoolStatus.mockResolvedValue({ json: pool([without(member, "id")]) });
    render(<StoragePage />);
    const button = (await screen.findByRole("button", { name: "Inspect affected paths" })) as HTMLButtonElement;
    expect(button.disabled).toBe(true);
    fireEvent.click(button);
    expect(client.getPoolAffected).not.toHaveBeenCalled();
  });
});

describe("Storage page filesystem activity", () => {
  it("shows a received sample, then unavailable when the stream ends", async () => {
    let end!: () => void;
    const ended = new Promise<void>((resolve) => (end = resolve));
    client.streamStats.mockImplementationOnce(() =>
      (async function* () {
        yield sample;
        await ended;
      })(),
    );
    render(<StoragePage />);
    await screen.findByText("Logical bytes written");
    expect(rowValue("Logical bytes written")).toBe("5.0 GiB (5,368,709,120 bytes)");
    expect(rowValue("Inodes used")).toBe("0");
    end();
    expect(await screen.findByText(/Stats stream ended/)).toBeTruthy();
    expect(screen.queryByText("Logical bytes written")).toBeNull();
  });

  it("renders a stats RPC failure as unavailable with no counter rows", async () => {
    client.streamStats.mockImplementationOnce(() =>
      failingStream(new ConnectError("stats service down", Code.Unavailable)),
    );
    render(<StoragePage />);
    expect(await screen.findByText(/stats service down/)).toBeTruthy();
    expect(screen.queryByText("Logical bytes written")).toBeNull();
  });

  async function showSample(next: Sample) {
    client.streamStats.mockImplementationOnce((_request: unknown, options?: { signal?: AbortSignal }) =>
      (async function* () {
        yield next;
        yield* openStream(_request, options);
      })(),
    );
    render(<StoragePage />);
    await screen.findByText("Logical bytes written");
  }

  it("renders a counter the server omitted as unavailable, never as 0", async () => {
    await showSample({ bytesWritten: 4096n, bytesRead: 0n });
    for (const label of ["Logical storage used", "Inodes used"]) {
      expect(rowValue(label)).toMatch(/^Unavailable · .*not reported/);
      expect(rowValue(label)).not.toMatch(/\b0\b/);
    }
    expect(rowValue("Logical bytes written")).toBe("4.0 KiB (4,096 bytes)");
    expect(rowValue("Logical bytes read")).toBe("0 bytes");
  });

  it("renders a reported zero as 0 and a nonzero count as its value", async () => {
    await showSample({ bytesWritten: 0n, bytesRead: 3n * 1024n ** 2n, usedBytes: 0n, usedInodes: 12_345n });
    expect(rowValue("Logical bytes written")).toBe("0 bytes");
    expect(rowValue("Logical storage used")).toBe("0 bytes");
    expect(rowValue("Logical bytes read")).toBe("3.0 MiB (3,145,728 bytes)");
    expect(rowValue("Inodes used")).toBe("12,345");
    expect(document.body.textContent).not.toMatch(/Unavailable · .*not reported/);
    // The fix makes the old caveat untrue, so the page must not print it.
    expect(document.body.textContent).not.toMatch(/can mean zero or not reported/);
  });
});

describe("Storage page affected-path scan", () => {
  async function inspect() {
    render(<StoragePage />);
    fireEvent.click(await screen.findByRole("button", { name: "Inspect affected paths" }));
    await waitFor(() => expect(client.getPoolAffected).toHaveBeenCalledTimes(1));
  }

  it("never shows an incomplete scan's path total as zero", async () => {
    client.getPoolAffected.mockResolvedValue({
      json: JSON.stringify({
        member: "hdd-a",
        complete: false,
        errors: [],
        coverage: "partial",
        views: [{ view: "live", unresolved_segments: [], references: [] }],
      }),
    });
    await inspect();
    await screen.findByText("Scan complete");
    expect(rowValue("Scan complete")).toBe("No");
    expect(rowValue("Reported path total across views")).toMatch(/^Unavailable/);
    expect(rowValue("Reported path total across views")).not.toMatch(/\b0\b/);
  });

  it("renders an affected-path RPC failure as unavailable", async () => {
    client.getPoolAffected.mockRejectedValue(new ConnectError("scan timed out", Code.DeadlineExceeded));
    await inspect();
    expect(await screen.findByText(/scan timed out/)).toBeTruthy();
    expect(screen.queryByText("Reported path total across views")).toBeNull();
  });
});

describe("Storage page capacity and health summary", () => {
  it("draws the capacity bar only when all four byte counts are known and consistent", async () => {
    render(<StoragePage />);
    await screen.findByText("hdd-a");
    expect(screen.getByRole("img", { name: /Share of configured capacity/ })).toBeTruthy();
    expect(rowValue("Held back")).toBe("931.3 GiB (999,999,998,976 bytes)");
    cleanup();
    client.getPoolStatus.mockResolvedValue({ json: pool([without(member, "used_bytes")]) });
    render(<StoragePage />);
    await screen.findByText("hdd-a");
    expect(screen.queryByRole("img", { name: /Share of configured capacity/ })).toBeNull();
    expect(screen.getByText(/^Capacity bar unavailable/)).toBeTruthy();
  });

  it("says no issues only when every health observation was reported empty", async () => {
    render(<StoragePage />);
    await screen.findByText("hdd-a");
    expect(screen.getByText("Health not observed yet")).toBeTruthy();
    expect(screen.queryByText("No health issues reported")).toBeNull();
    cleanup();
    client.getPoolStatus.mockResolvedValue({
      json: pool([{ ...member, health: { ...member.health, observed_ms: 1_800_000_000_000 } }]),
    });
    render(<StoragePage />);
    expect(await screen.findByText("No health issues reported")).toBeTruthy();
    cleanup();
    client.getPoolStatus.mockResolvedValue({
      json: pool([{ ...member, health: { ...member.health, warning: "reallocated sectors" } }]),
    });
    render(<StoragePage />);
    expect(await screen.findByText("1 health issue reported")).toBeTruthy();
    expect(screen.getByText("reallocated sectors")).toBeTruthy();
    cleanup();
    client.getPoolStatus.mockResolvedValue({ json: pool([without(member, "health")]) });
    render(<StoragePage />);
    expect(await screen.findByText("Health not fully reported")).toBeTruthy();
    expect(screen.queryByText("No health issues reported")).toBeNull();
  });
});

describe("Storage page affected-path display", () => {
  const scan = (views: unknown[]) =>
    JSON.stringify({ member: "hdd-a", complete: true, errors: [], coverage: "snapshots", views });
  const movie = [47, 77, 111, 118, 105, 101];
  async function inspect() {
    render(<StoragePage />);
    fireEvent.click(await screen.findByRole("button", { name: "Inspect affected paths" }));
  }

  it("shows one row per path with its extent count and folds views that report nothing", async () => {
    client.getPoolAffected.mockResolvedValue({
      json: scan([
        {
          view: "live",
          unresolved_segments: [],
          references: [0, 1, 2].map((extent) => ({ inode: 4, extent, segment: "segments/1", paths: [movie] })),
        },
        { view: "checkpoint one", unresolved_segments: [], references: [] },
        { view: "checkpoint two", unresolved_segments: [], references: [] },
      ]),
    });
    await inspect();
    expect(await screen.findByText("3 extents")).toBeTruthy();
    expect(screen.getAllByText("/Movie")).toHaveLength(1);
    expect(screen.getByText("2 views report no affected paths")).toBeTruthy();
  });

  it("refuses to draw an answer too large for the browser and says why", async () => {
    client.getPoolAffected.mockResolvedValue({ json: " ".repeat(8 * 1024 * 1024 + 1) });
    await inspect();
    expect(await screen.findByText(/too large to display/)).toBeTruthy();
    expect(screen.queryByText("Scan complete")).toBeNull();
  });
});

describe("Storage page monitoring limits", () => {
  const observed = { ...member.health, observed_ms: 1_800_000_000_000 };
  async function show(health: Record<string, unknown>, extra: Record<string, unknown> = {}) {
    cleanup();
    client.getPoolStatus.mockResolvedValue({ json: pool([{ ...member, ...extra, health }]) });
    render(<StoragePage />);
    await screen.findAllByText("hdd-a");
  }

  it("never shows the healthy state while a monitor is degraded, silent or unreported", async () => {
    await show(observed);
    expect(screen.getByText("No health issues reported")).toBeTruthy();
    expect(screen.getByText(/^Kernel error coverage: current boot kernel ring/)).toBeTruthy();

    await show({ ...observed, kernel_coverage: "kernel watcher unavailable: Permission denied" });
    expect(screen.getByText("Monitoring or evacuation is limited")).toBeTruthy();
    expect(screen.getByText("kernel watcher unavailable: Permission denied")).toBeTruthy();
    expect(screen.queryByText("No health issues reported")).toBeNull();

    await show({ ...observed, kernel_coverage: null });
    expect(screen.getByText("Monitoring or evacuation is limited")).toBeTruthy();
    expect(screen.queryByText("No health issues reported")).toBeNull();

    await show(without(observed, "kernel_coverage"));
    expect(screen.getByText("Health not fully reported")).toBeTruthy();
    expect(screen.queryByText("No health issues reported")).toBeNull();

    await show({ ...observed, last_sample: null });
    expect(screen.getByText("Monitoring or evacuation is limited")).toBeTruthy();
    expect(screen.getByText("no sample recorded")).toBeTruthy();

    await show(observed, { evacuation_capacity: { sufficient: true, unavailable_members: ["hdd-a"] } });
    expect(screen.getByText("1 health issue reported")).toBeTruthy();
    expect(screen.getByText(/storage binding or free-space check failed/)).toBeTruthy();
    expect(screen.queryByText("No health issues reported")).toBeNull();

    await show(observed, { evacuation_capacity: { sufficient: true, unavailable_members: ["hdd-b"] } });
    expect(screen.getByText("No health issues reported")).toBeTruthy();

    await show(observed, { evacuation_capacity: { sufficient: false, unavailable_members: [] } });
    expect(screen.getByText("Monitoring or evacuation is limited")).toBeTruthy();
    expect(screen.queryByText("No health issues reported")).toBeNull();

    await show(observed, { evacuation_capacity: { sufficient: true } });
    expect(screen.getByText("Health not fully reported")).toBeTruthy();
    expect(screen.queryByText("No health issues reported")).toBeNull();

    await show(observed, { unreadable_objects: 3 });
    expect(screen.getByText("1 health issue reported")).toBeTruthy();
    expect(screen.queryByText("No health issues reported")).toBeNull();
  });

  it("shows the observation's age and stops calling a member healthy once it is stale", async () => {
    await show(observed);
    expect(screen.getByText("No health issues reported")).toBeTruthy();
    expect(screen.getByText(/^Last health observation .*, 1 min before this status$/)).toBeTruthy();

    // Five minutes is the bound; a monitor that refreshed on schedule would have observed again by then.
    await show({ ...observed, observed_ms: generatedMs - 5 * 60_000 - 1 });
    expect(screen.getByText("Monitoring or evacuation is limited")).toBeTruthy();
    expect(screen.getByText(/^last made 5 min before this status/)).toBeTruthy();
    expect(screen.queryByText("No health issues reported")).toBeNull();

    await show({ ...observed, observed_ms: generatedMs - 3 * 86_400_000 });
    expect(screen.getByText(/^last made 3 d before this status/)).toBeTruthy();
    expect(screen.queryByText("No health issues reported")).toBeNull();

    // A server without the status time cannot say how old the observation is.
    cleanup();
    client.getPoolStatus.mockResolvedValue({
      json: pool([{ ...member, health: observed }], [], { generated_ms: undefined }),
    });
    render(<StoragePage />);
    await screen.findAllByText("hdd-a");
    expect(screen.getByText("Health not fully reported")).toBeTruthy();
    expect(screen.queryByText("No health issues reported")).toBeNull();
  });
});

describe("Storage page segment faults", () => {
  const fault = (n: number, extra: Record<string, unknown> = {}) => ({
    location: `segments/${n}`,
    physical_key: `hdd-a/segments/${n}`,
    member_id: "hdd-a",
    reason: "checksum mismatch",
    observed_ms: 1_800_000_000_000,
    attempts: 2,
    next_retry_ms: 1_800_000_240_000,
    ...extra,
  });
  async function show(faults: unknown[], health: Record<string, unknown> = member.health) {
    client.getPoolStatus.mockResolvedValue({
      json: pool([{ ...member, health, unreadable_objects: faults.length }], faults),
    });
    render(<StoragePage />);
    await screen.findByText("hdd-a");
  }

  it("lists a member's fault with its location, reason, attempts and next retry time", async () => {
    await show([fault(7, { attempts: 1 })]);
    const list = screen.getByRole("list", { name: "Unreadable segment faults" });
    expect(list.closest("section")?.textContent).toContain("hdd-a");
    expect(list.textContent).toContain("segments/7");
    expect(list.textContent).toContain("checksum mismatch");
    expect(list.textContent).toContain("1 attempt ·");
    expect(list.textContent).toContain(`next retry ${new Date(1_800_000_240_000).toLocaleString()}`);
    expect(screen.queryByText(/more faults? not listed/)).toBeNull();
  });

  it("shows when a quarantined member's quarantine began", async () => {
    await show([], { ...member.health, quarantine: "SMART media failure", since_ms: 1_700_000_000_000 });
    expect(screen.getByText("SMART media failure")).toBeTruthy();
    expect(screen.getByText(`Quarantined since ${new Date(1_700_000_000_000).toLocaleString()}`)).toBeTruthy();
  });

  it("caps a long fault list and says how many it left out", async () => {
    await show(Array.from({ length: 1203 }, (_, n) => fault(n)));
    const list = screen.getByRole("list", { name: "Unreadable segment faults" });
    expect(list.querySelectorAll("li")).toHaveLength(50);
    expect(screen.getByText("and 1,153 more faults not listed")).toBeTruthy();
    expect(rowValue("Unreadable objects")).toBe("1,203");
  });

  it("renders the fault details unavailable when one entry is malformed, rather than dropping it", async () => {
    await show([fault(1), without(fault(2), "attempts")]);
    expect(screen.queryByRole("list", { name: "Unreadable segment faults" })).toBeNull();
    expect(screen.getByText(/^Segment fault details unavailable · unreadable: attempts was not reported/)).toBeTruthy();
    expect(screen.queryByText("No health issues reported")).toBeNull();
  });

  it("lists a fault whose member id names no listed member in its own section", async () => {
    await show([fault(9, { member_id: "authority" })]);
    const list = screen.getByRole("list", { name: "Unattributed segment faults" });
    expect(list.textContent).toContain("segments/9");
    expect(screen.getByText("1 unreadable segment record outside the listed members")).toBeTruthy();
    expect(screen.queryByRole("list", { name: "Unreadable segment faults" })).toBeNull();
  });
});
