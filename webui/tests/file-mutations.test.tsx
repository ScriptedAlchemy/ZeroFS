import { act, renderHook, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useUploads } from "../src/components/file-manager/hooks/use-uploads";
import { p9client, ZeroFsClient } from "../src/lib/zerofs/client";

const connect = vi.hoisted(() => vi.fn());
vi.mock("../src/generated/zerofs-client/zerofs_client", () => ({
  default: vi.fn().mockResolvedValue(undefined),
  Client: { connect },
}));

function errno(code: number) {
  return Object.assign(new Error(`errno ${code}`), { ecode: code });
}

async function openSession() {
  const file = {
    writeAt: vi.fn().mockResolvedValue(undefined),
    setLen: vi.fn().mockResolvedValue(undefined),
    syncAll: vi.fn().mockResolvedValue(undefined),
    close: vi.fn().mockResolvedValue(undefined),
  };
  const fs = {
    capabilities: () => ({ max_read_chunk: 4, max_write_chunk: 4 }),
    trafficStats: () => ({ bytes_sent: 0, bytes_received: 0, operations: 0 }),
    openFile: vi.fn().mockResolvedValue(file),
    rename: vi.fn().mockResolvedValue(undefined),
    renameNoReplace: vi.fn().mockResolvedValue(undefined),
    removeFile: vi.fn().mockResolvedValue(undefined),
    sync: vi.fn().mockResolvedValue(undefined),
    close: vi.fn().mockResolvedValue(undefined),
  };
  connect.mockResolvedValue(fs);
  const client = new ZeroFsClient();
  await client.connect("ws://test/ws/9p");
  return { client, fs, file };
}

afterEach(() => vi.restoreAllMocks());

describe("File Manager namespace mutations", () => {
  it("reports a destination that appears during upload and removes the staged file", async () => {
    const { client, fs, file } = await openSession();
    try {
      const exists = errno(17);
      fs.renameNoReplace.mockRejectedValue(exists);
      await expect(client.uploadBlob("/Report", new Blob(["text"]))).rejects.toBe(exists);
      const temporary = fs.openFile.mock.calls[0][0];
      expect(fs.renameNoReplace).toHaveBeenCalledWith(temporary, "/Report");
      expect(fs.rename).not.toHaveBeenCalled();
      expect(file.close).toHaveBeenCalledOnce();
      expect(fs.removeFile).toHaveBeenCalledWith(temporary);
    } finally {
      await client.close();
    }
  });

  it("publishes with replacement only after approval and leaves case-only rename to the server", async () => {
    const { client, fs } = await openSession();
    try {
      await client.rename("/report", "/Report");
      expect(fs.renameNoReplace).toHaveBeenCalledWith("/report", "/Report");
      await client.uploadBlob("/Report", new Blob(["text"]), { replace: true });
      expect(fs.rename).toHaveBeenCalledWith(fs.openFile.mock.calls[0][0], "/Report");
      expect(fs.removeFile).not.toHaveBeenCalled();
    } finally {
      await client.close();
    }
  });

  it("creates a new empty file exclusively", async () => {
    const { client, fs } = await openSession();
    try {
      fs.openFile.mockRejectedValue(errno(17));
      await expect(client.createFile("/Report")).rejects.toMatchObject({ ecode: 17 });
      expect(fs.openFile).toHaveBeenCalledWith("/Report", false, true, true, true, false, 0o644);
      expect(fs.rename).not.toHaveBeenCalled();
    } finally {
      await client.close();
    }
  });

  it.each(["overwrite", "skip", "cancel"] as const)("honors %s for server-reported name collisions", async (answer) => {
    vi.spyOn(p9client, "stat").mockImplementation(async (path) => {
      if (path === "/Report") return {} as Awaited<ReturnType<typeof p9client.stat>>;
      throw errno(2);
    });
    const upload = vi.spyOn(p9client, "uploadBlob").mockResolvedValue(undefined);
    vi.spyOn(p9client, "finishUploadBatch").mockResolvedValue(undefined);
    const queries = new QueryClient();
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={queries}>{children}</QueryClientProvider>
    );
    const { result, unmount } = renderHook(() => useUploads("/"), { wrapper });
    const transfer = new DataTransfer();
    transfer.items.add(new File(["x"], "Report"));
    transfer.items.add(new File(["y"], "new"));
    let done: Promise<void>;
    act(() => {
      done = result.current.uploadFiles(transfer.files);
    });
    await waitFor(() => expect(result.current.overwritePrompt?.existing).toEqual(["Report"]));
    await act(async () => {
      result.current.overwritePrompt!.resolve(answer);
      await done;
    });
    const destinations = Object.fromEntries(upload.mock.calls.map(([path, , options]) => [path, options?.replace]));
    expect(destinations).toEqual(
      answer === "overwrite" ? { "/Report": true, "/new": false } : answer === "skip" ? { "/new": false } : {},
    );
    unmount();
    queries.clear();
  });
});

describe("File Preview save", () => {
  it("edits the original handle before shortening it and waits for durability", async () => {
    const { client, fs, file } = await openSession();
    try {
      await client.saveFile("/existing", new Uint8Array([1, 2, 3, 4, 5]));
      expect(fs.openFile).toHaveBeenCalledWith("/existing", false, true, false, false, false, 0);
      expect(file.writeAt.mock.calls.map(([offset]) => offset)).toEqual([0n, 4n]);
      expect(file.setLen).toHaveBeenCalledWith(5n);
      expect(file.writeAt.mock.invocationCallOrder[1]).toBeLessThan(file.setLen.mock.invocationCallOrder[0]);
      expect(file.setLen.mock.invocationCallOrder[0]).toBeLessThan(file.syncAll.mock.invocationCallOrder[0]);
      expect(file.syncAll.mock.invocationCallOrder[0]).toBeLessThan(file.close.mock.invocationCallOrder[0]);
      expect(fs.rename).not.toHaveBeenCalled();
    } finally {
      await client.close();
    }
  });

  it("leaves the existing length intact on a failed write and surfaces a failed sync", async () => {
    const { client, file } = await openSession();
    try {
      const stale = errno(116);
      file.writeAt.mockRejectedValueOnce(stale);
      await expect(client.saveFile("/existing", new Uint8Array([1]))).rejects.toBe(stale);
      expect(file.setLen).not.toHaveBeenCalled();
      file.syncAll.mockRejectedValueOnce(stale);
      await expect(client.saveFile("/existing", new Uint8Array())).rejects.toBe(stale);
      expect(file.setLen).toHaveBeenCalledWith(0n);
      expect(file.close).toHaveBeenCalledTimes(2);
    } finally {
      await client.close();
    }
  });
});
