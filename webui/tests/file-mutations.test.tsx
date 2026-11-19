import { act, renderHook, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useUploads } from "../src/components/file-manager/hooks/use-uploads";
import { p9client, ZeroFsClient } from "../src/lib/zerofs/client";
import { TransferContext, type TransferHandle } from "../src/components/transfers/TransferContext";

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

  it("closes completed batch handles if the connection closes before the durability barrier", async () => {
    const { client, file } = await openSession();
    const batch = client.beginUploadBatch();
    try {
      await client.uploadBlob("/Report", new Blob(["text"]), { batch });
      expect(file.close).not.toHaveBeenCalled();
      await client.close();
      await expect(client.finishUploadBatch(batch)).rejects.toThrow("ZeroFS client is not connected");
      expect(file.close).toHaveBeenCalledOnce();
      expect(batch.files.size).toBe(0);
      await client.finishUploadBatch(batch);
      expect(file.close).toHaveBeenCalledOnce();
    } finally {
      await client.close();
    }
  });

  it.each([false, true])("does not publish an old upload through a replacement session (replace=%s)", async (replace) => {
    const { client, fs, file } = await openSession();
    let finishWrite!: () => void;
    file.writeAt.mockReturnValueOnce(new Promise<void>((resolve) => { finishWrite = resolve; }));
    const uploading = client.uploadBlob("/Report", new Blob(["text"]), { replace });
    const outcome = uploading.catch((error: unknown) => error);
    try {
      await waitFor(() => expect(file.writeAt).toHaveBeenCalledOnce());
      await client.close();
      const replacement = {
        ...fs,
        rename: vi.fn().mockResolvedValue(undefined),
        renameNoReplace: vi.fn().mockResolvedValue(undefined),
      };
      connect.mockResolvedValue(replacement);
      await client.connect("ws://restored/ws/9p");
      finishWrite();
      expect(await outcome).toBeInstanceOf(Error);
      expect(await outcome).toHaveProperty("message", expect.stringMatching(/session/i));
      expect(replacement.rename).not.toHaveBeenCalled();
      expect(replacement.renameNoReplace).not.toHaveBeenCalled();
      expect(fs.rename).not.toHaveBeenCalled();
      expect(fs.renameNoReplace).not.toHaveBeenCalled();
      expect(file.close).toHaveBeenCalledOnce();
      expect(fs.removeFile).toHaveBeenCalledWith(fs.openFile.mock.calls[0][0]);
    } finally {
      finishWrite?.();
      await outcome;
      await client.close();
    }
  });

  it("does not acknowledge an old batch using a replacement session's durability barrier", async () => {
    const { client, fs, file } = await openSession();
    const batch = client.beginUploadBatch();
    try {
      await client.uploadBlob("/Report", new Blob(["text"]), { batch });
      await client.close();
      const replacement = { ...fs, sync: vi.fn().mockResolvedValue(undefined) };
      connect.mockResolvedValue(replacement);
      await client.connect("ws://restored/ws/9p");
      await expect(client.finishUploadBatch(batch)).rejects.toThrow(/session/i);
      expect(replacement.sync).not.toHaveBeenCalled();
      expect(file.close).toHaveBeenCalledOnce();
      expect(batch.files.size).toBe(0);
    } finally {
      await client.close();
    }
  });

  it("rejects adding a replacement session's upload to an unfinished old batch", async () => {
    const { client, fs } = await openSession();
    const batch = client.beginUploadBatch();
    try {
      await client.uploadBlob("/Report", new Blob(["text"]), { batch });
      await client.close();
      const replacement = { ...fs, openFile: vi.fn() };
      connect.mockResolvedValue(replacement);
      await client.connect("ws://restored/ws/9p");
      await expect(client.uploadBlob("/Other", new Blob(["text"]), { batch })).rejects.toThrow(/session/i);
      expect(replacement.openFile).not.toHaveBeenCalled();
    } finally {
      await client.finishUploadBatch(batch).catch(() => {});
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

  it.each(["overwrite", "skip", "cancel"] as const)("rechecks a published upload before retry and honors %s", async (answer) => {
    const stat = vi.spyOn(p9client, "stat").mockRejectedValue(errno(2));
    const upload = vi.spyOn(p9client, "uploadBlob").mockResolvedValue(undefined);
    vi.spyOn(p9client, "finishUploadBatch").mockRejectedValue(new Error("durability barrier failed"));
    const handles: (TransferHandle & { retry?: () => void })[] = [];
    const startUpload = (_name: string, _total: number, retry?: () => void): TransferHandle => {
      const handle = {
        id: handles.length, signal: new AbortController().signal, retry,
        setName: vi.fn(), update: vi.fn(), finish: vi.fn(), fail: vi.fn(),
      };
      handles.push(handle);
      return handle;
    };
    const queries = new QueryClient();
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={queries}>
        <TransferContext value={{ startUpload, startDownload: () => { throw new Error("unexpected download"); }, startDelete: () => { throw new Error("unexpected delete"); } }}>
          {children}
        </TransferContext>
      </QueryClientProvider>
    );
    const { result, unmount } = renderHook(() => useUploads("/"), { wrapper });
    const transfer = new DataTransfer();
    transfer.items.add(new File(["original"], "Report"));
    await act(async () => result.current.uploadFiles(transfer.files));
    expect(handles[0].fail).toHaveBeenCalledWith("durability barrier failed");
    expect(upload).toHaveBeenCalledOnce();

    // Publication was acknowledged before the barrier failed. The destination can
    // now be this upload or a concurrent writer's replacement; require approval.
    stat.mockResolvedValue({} as Awaited<ReturnType<typeof p9client.stat>>);
    let done: Promise<void>;
    act(() => {
      done = Promise.resolve(handles[0].retry!());
    });
    await waitFor(() => expect(result.current.overwritePrompt?.existing).toEqual(["Report"]));
    expect(upload).toHaveBeenCalledOnce();
    await act(async () => {
      result.current.overwritePrompt!.resolve(answer);
      await done;
    });
    if (answer === "overwrite") {
      expect(upload).toHaveBeenCalledTimes(2);
      expect(upload.mock.calls[1][2]).toMatchObject({ replace: true, batch: undefined });
      expect(handles[1].finish).toHaveBeenCalledOnce();
    } else {
      expect(upload).toHaveBeenCalledOnce();
      expect(handles).toHaveLength(1);
    }
    unmount();
    queries.clear();
  });

  it.each(["unmounts", "changes directory"] as const)("cancels an outstanding overwrite decision when its file tab %s", async (action) => {
    vi.spyOn(p9client, "stat").mockResolvedValue({} as Awaited<ReturnType<typeof p9client.stat>>);
    const upload = vi.spyOn(p9client, "uploadBlob").mockResolvedValue(undefined);
    const queries = new QueryClient();
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={queries}>{children}</QueryClientProvider>
    );
    const { result, unmount, rerender } = renderHook(({ path }) => useUploads(path), {
      wrapper, initialProps: { path: "/" },
    });
    const transfer = new DataTransfer();
    transfer.items.add(new File(["text"], "Report"));
    let done: Promise<void>;
    act(() => {
      done = result.current.uploadFiles(transfer.files);
    });
    await waitFor(() => expect(result.current.overwritePrompt?.existing).toEqual(["Report"]));
    if (action === "unmounts") unmount();
    else rerender({ path: "/other" });
    await done!;
    expect(upload).not.toHaveBeenCalled();
    if (action !== "unmounts") {
      expect(result.current.overwritePrompt).toBeNull();
      unmount();
    }
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
