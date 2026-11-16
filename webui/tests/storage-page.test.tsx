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
  health: { quarantine: null, monitoring_error: null, warning: null, evacuation_error: null, observed_ms: null },
};
const pool = (members: unknown[]) =>
  JSON.stringify({
    pool_id: "test-pool",
    members,
    pending_operations: 0,
    mapped_objects: 3,
    unreadable: [],
    quarantined_leftovers: [],
  });
const sample = { bytesWritten: 5n * 1024n ** 3n, bytesRead: 0n, usedBytes: 2048n, usedInodes: 0n };

// A stream that stays open until the page aborts it.
function openStream(_request: unknown, options?: { signal?: AbortSignal }): AsyncIterable<typeof sample> {
  return {
    [Symbol.asyncIterator]: () => ({
      next: () =>
        new Promise((_resolve, reject) =>
          options?.signal?.addEventListener("abort", () => reject(new Error("aborted")), { once: true }),
        ),
    }),
  };
}

function failingStream(error: Error): AsyncIterable<typeof sample> {
  return { [Symbol.asyncIterator]: () => ({ next: () => Promise.reject(error) }) };
}

function without(record: Record<string, unknown>, ...keys: string[]) {
  return Object.fromEntries(Object.entries(record).filter(([key]) => !keys.includes(key)));
}

function rowValue(label: string): string {
  const term = screen.getByText(label, { selector: "dt" });
  return term.nextElementSibling?.textContent ?? "";
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
      expect(rowValue(label)).toMatch(/^unavailable\./);
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
  it("shows a received sample with the zero-or-missing note, then unavailable when the stream ends", async () => {
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
    expect(screen.getByText(/A 0 here can mean zero or not reported/)).toBeTruthy();
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
    expect(screen.queryByText(/A 0 here can mean zero/)).toBeNull();
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
    expect(rowValue("Scan complete")).toBe("no");
    expect(rowValue("Reported path total across views")).toMatch(/^unavailable\./);
    expect(rowValue("Reported path total across views")).not.toMatch(/\b0\b/);
  });

  it("renders an affected-path RPC failure as unavailable", async () => {
    client.getPoolAffected.mockRejectedValue(new ConnectError("scan timed out", Code.DeadlineExceeded));
    await inspect();
    expect(await screen.findByText(/scan timed out/)).toBeTruthy();
    expect(screen.queryByText("Reported path total across views")).toBeNull();
  });
});
