import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { p9client, ZeroFsClient } from "../src/lib/zerofs/client";

const connect = vi.hoisted(() => vi.fn());
vi.mock("../src/generated/zerofs-client/zerofs_client", () => ({
  default: vi.fn().mockResolvedValue(undefined),
  Client: { connect },
}));

function session() {
  return {
    connected: true,
    capabilities: () => ({ max_read_chunk: 4, max_write_chunk: 4 }),
    trafficStats: () => ({ bytes_sent: 0, bytes_received: 0, operations: 0 }),
    createDir: vi.fn().mockResolvedValue(undefined),
    close: vi.fn().mockResolvedValue(undefined),
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((done, fail) => { resolve = done; reject = fail; });
  return { promise, resolve, reject };
}

async function settleMicrotasks() {
  for (let i = 0; i < 8; i++) await Promise.resolve();
}

function pageEvent(type: "pagehide" | "pageshow", persisted: boolean) {
  const event = new Event(type);
  Object.defineProperty(event, "persisted", { value: persisted });
  window.dispatchEvent(event);
}

beforeEach(async () => {
  await p9client.close();
  connect.mockReset();
  vi.useFakeTimers();
});

afterEach(async () => {
  await p9client.close();
  vi.useRealTimers();
});

it("closes a late connection instead of reviving a closed client", async () => {
  const pending = deferred<ReturnType<typeof session>>();
  const late = session();
  connect.mockReturnValue(pending.promise);
  const client = new ZeroFsClient();
  const connecting = client.connect("ws://old/ws/9p");
  const outcome = connecting.catch((error: unknown) => error);
  await settleMicrotasks();
  expect(connect).toHaveBeenCalledOnce();
  await client.close();
  pending.resolve(late);
  expect(await outcome).toMatchObject({ name: "AbortError" });
  expect(client.state).toBe("disconnected");
  expect(late.close).toHaveBeenCalledOnce();
  expect(vi.getTimerCount()).toBe(0);
});

it("keeps a new session when an older connect finishes after close and reconnect", async () => {
  const pending = deferred<ReturnType<typeof session>>();
  const late = session();
  const current = session();
  connect.mockReturnValueOnce(pending.promise).mockResolvedValueOnce(current);
  const client = new ZeroFsClient();
  const old = client.connect("ws://old/ws/9p").catch((error: unknown) => error);
  await settleMicrotasks();
  await client.close();
  const reopened = client.connect("ws://new/ws/9p");
  await settleMicrotasks();
  expect(connect.mock.calls.map(([url]) => url)).toEqual(["ws://old/ws/9p", "ws://new/ws/9p"]);
  await reopened;
  pending.resolve(late);
  expect(await old).toMatchObject({ name: "AbortError" });
  expect(client.state).toBe("connected");
  expect(late.close).toHaveBeenCalledOnce();
  expect(current.close).not.toHaveBeenCalled();
  await client.mkdir("/current");
  expect(current.createDir).toHaveBeenCalledWith("/current", 0o755);
  expect(late.createDir).not.toHaveBeenCalled();
  await client.close();
  expect(vi.getTimerCount()).toBe(0);
});

it("does not clear or retry a pending replacement when a superseded attempt fails", async () => {
  const first = deferred<ReturnType<typeof session>>();
  const second = deferred<ReturnType<typeof session>>();
  const current = session();
  const failure = new Error("old handshake failed");
  connect.mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise);
  const client = new ZeroFsClient();
  const old = client.connect("ws://old/ws/9p").catch((error: unknown) => error);
  await settleMicrotasks();
  await client.close();
  const replacement = client.connect("ws://new/ws/9p");
  await settleMicrotasks();
  first.reject(failure);
  expect(await old).toBe(failure);
  const sameReplacement = client.connect("ws://new/ws/9p");
  await settleMicrotasks();
  expect(connect).toHaveBeenCalledTimes(2);
  expect(client.state).toBe("connecting");
  expect(vi.getTimerCount()).toBe(0);
  second.resolve(current);
  await Promise.all([replacement, sameReplacement]);
  expect(client.state).toBe("connected");
  await client.close();
});

it("retires a connection whose connected-state observer immediately closes it", async () => {
  const connected = session();
  connect.mockResolvedValue(connected);
  const client = new ZeroFsClient();
  client.onStateChange((state) => { if (state === "connected") void client.close(); });
  await client.connect("ws://page/ws/9p");
  expect(client.state).toBe("disconnected");
  expect(connected.close).toHaveBeenCalledOnce();
  expect(vi.getTimerCount()).toBe(0);
});

it("restores an enabled connection when the page returns from the back-forward cache", async () => {
  const first = session();
  const restored = session();
  connect.mockResolvedValueOnce(first).mockResolvedValueOnce(restored);
  p9client.enableAutoReconnect("ws://page/ws/9p");
  await settleMicrotasks();
  expect(p9client.state).toBe("connected");
  pageEvent("pagehide", true);
  await settleMicrotasks();
  expect(first.close).toHaveBeenCalledOnce();
  expect(p9client.state).toBe("disconnected");
  pageEvent("pageshow", true);
  await settleMicrotasks();
  expect(connect).toHaveBeenCalledTimes(2);
  expect(connect).toHaveBeenLastCalledWith("ws://page/ws/9p");
  expect(p9client.state).toBe("connected");
  await p9client.mkdir("/restored");
  expect(restored.createDir).toHaveBeenCalledWith("/restored", 0o755);
});

it("does not start the file transport when an operator-only page is restored", async () => {
  pageEvent("pagehide", true);
  pageEvent("pageshow", true);
  await settleMicrotasks();
  expect(connect).not.toHaveBeenCalled();
  expect(p9client.state).toBe("disconnected");
  expect(vi.getTimerCount()).toBe(0);
});

it("keeps the session until navigation commits and does not restore a discarded page", async () => {
  const connected = session();
  connect.mockResolvedValue(connected);
  p9client.enableAutoReconnect("ws://page/ws/9p");
  await settleMicrotasks();
  window.dispatchEvent(new Event("beforeunload"));
  expect(p9client.state).toBe("connected");
  expect(connected.close).not.toHaveBeenCalled();
  pageEvent("pagehide", false);
  pageEvent("pageshow", true);
  await settleMicrotasks();
  expect(connected.close).toHaveBeenCalledOnce();
  expect(connect).toHaveBeenCalledOnce();
  expect(p9client.state).toBe("disconnected");
});

it("keeps the existing initial reconnect delay for the current connection", async () => {
  const current = session();
  connect.mockRejectedValueOnce(new Error("temporarily unavailable")).mockResolvedValueOnce(current);
  p9client.enableAutoReconnect("ws://page/ws/9p");
  await settleMicrotasks();
  expect(p9client.state).toBe("disconnected");
  await vi.advanceTimersByTimeAsync(499);
  expect(connect).toHaveBeenCalledOnce();
  await vi.advanceTimersByTimeAsync(1);
  expect(connect).toHaveBeenCalledTimes(2);
  expect(p9client.state).toBe("connected");
});
