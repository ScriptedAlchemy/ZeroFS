import { afterEach, describe, expect, it, vi } from "vitest";
import { ZeroFsClient } from "../src/lib/zerofs/client";

const connect = vi.hoisted(() => vi.fn());
vi.mock("../src/generated/zerofs-client/zerofs_client", () => ({
  default: vi.fn().mockResolvedValue(undefined),
  Client: { connect },
}));

const clients = new Set<ZeroFsClient>();
afterEach(async () => {
  await Promise.all([...clients].map((client) => client.close()));
  clients.clear();
  vi.restoreAllMocks();
});

async function permissionTree(options: { failAt?: string; afterSet?: () => void } = {}) {
  // All nodes belong to the configured non-root WebUI identity. Path-based
  // operations must search each ancestor, even when the target is owned.
  const nodes = new Map([
    ["/tree", { directory: true, mode: 0o700 }],
    ["/tree/nested", { directory: true, mode: 0o700 }],
    ["/tree/nested/file", { directory: false, mode: 0o600 }],
  ]);
  const denied = () => Object.assign(new Error("Permission denied"), { ecode: 13 });
  const searchAncestors = (path: string) => {
    const components = path.split("/").filter(Boolean);
    for (let length = 1; length < components.length; length++) {
      const ancestor = nodes.get(`/${components.slice(0, length).join("/")}`)!;
      if (!(ancestor.mode & 0o100)) throw denied();
    }
  };
  const fs = {
    capabilities: () => ({ max_read_chunk: 4, max_write_chunk: 4 }),
    trafficStats: () => ({ bytes_sent: 0, bytes_received: 0, operations: 0 }),
    close: vi.fn().mockResolvedValue(undefined),
    setAttr: vi.fn(async (path: string, mode: number) => {
      searchAncestors(path);
      if (path === options.failAt) throw denied();
      nodes.get(path)!.mode = mode;
      options.afterSet?.();
    }),
    openDir: vi.fn(async (path: string) => {
      searchAncestors(path);
      if (!(nodes.get(path)!.mode & 0o400)) throw denied();
      const children = [...nodes].filter(([child]) => child.slice(0, child.lastIndexOf("/")) === path);
      return {
        nextBatch: vi.fn().mockResolvedValueOnce(children.map(([child, node]) => {
          const name = child.slice(child.lastIndexOf("/") + 1);
          return {
            name, name_bytes: [...new TextEncoder().encode(name)], name_is_utf8: true,
            file_type: node.directory ? "Dir" : "File",
            metadata: { size: 0, mtime: { secs: 0 }, mode: node.mode, uid: 1000, gid: 1000, nlink: 1 },
          };
        })).mockResolvedValue([]),
        close: vi.fn().mockResolvedValue(undefined),
      };
    }),
  };
  connect.mockResolvedValue(fs);
  const client = new ZeroFsClient();
  clients.add(client);
  await client.connect("ws://test/ws/9p");
  return { client, nodes };
}

describe("recursive permissions", () => {
  it("updates descendants before removing search permission from their ancestors", async () => {
    const { client, nodes } = await permissionTree();
    await expect(client.setattrRecursive("/tree", { mode: 0 })).resolves.toEqual({ applied: 3, failed: 0 });
    expect([...nodes.values()].map((node) => node.mode)).toEqual([0, 0, 0]);
  });

  it("counts a denied child while still applying the requested directory permissions", async () => {
    const { client, nodes } = await permissionTree({ failAt: "/tree/nested/file" });
    await expect(client.setattrRecursive("/tree", { mode: 0 })).resolves.toEqual({ applied: 2, failed: 1 });
    expect([...nodes.values()].map((node) => node.mode)).toEqual([0, 0, 0o600]);
  });

  it("does not change ancestors after cancellation during a descendant update", async () => {
    const abort = new AbortController();
    const { client, nodes } = await permissionTree({ afterSet: () => abort.abort() });
    await expect(client.setattrRecursive("/tree", { mode: 0 }, abort.signal)).resolves.toEqual({ applied: 1, failed: 0 });
    expect([...nodes.values()].map((node) => node.mode)).toEqual([0o700, 0o700, 0]);
  });
});
