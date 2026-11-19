import { act, renderHook } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Unzip, UnzipInflate } from "fflate";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useUploads } from "../src/components/file-manager/hooks/use-uploads";
import { TransferContext, type TransferHandle } from "../src/components/transfers/TransferContext";
import { p9client, ZeroFsClient, type FileEntry } from "../src/lib/zerofs/client";

vi.mock("../src/generated/zerofs-client/zerofs_client", () => ({ default: vi.fn(), Client: {} }));

const entry = (name: string, isDir = false): FileEntry => ({
  name, isDir, resolvedIsDir: isDir, isSymlink: false, size: 1n, mtimeSec: 0n, mode: 0,
  uid: 0, gid: 0, nlink: 1n,
});

afterEach(() => vi.restoreAllMocks());

function downloadHook() {
  const abort = new AbortController();
  const handle: TransferHandle = {
    id: 0, signal: abort.signal, setName: vi.fn(), update: vi.fn(), finish: vi.fn(), fail: vi.fn(),
  };
  const queries = new QueryClient();
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queries}>
      <TransferContext value={{
        startDownload: () => handle,
        startUpload: () => { throw new Error("unexpected upload"); },
        startDelete: () => { throw new Error("unexpected delete"); },
      }}>
        {children}
      </TransferContext>
    </QueryClientProvider>
  );
  const hook = renderHook(() => useUploads("/"), { wrapper });
  return { ...hook, handle, abort, queries };
}

describe("folder downloads", () => {
  it.each(["file read", "nested directory listing"] as const)(
    "reports a failed %s instead of publishing an incomplete ZIP",
    async (failureAt) => {
      const failure = Object.assign(new Error("unreadable source"), { ecode: 5 });
      vi.spyOn(p9client, "directoryListing").mockImplementation(async (path) => {
        if (path === "/folder/nested") throw failure;
        return { entries: [entry("readable"), entry("nested", failureAt !== "file read")], rawNames: [] };
      });
      vi.spyOn(p9client, "readFile").mockImplementation(async (path) => {
        if (path.endsWith("/nested")) throw failure;
        return new Uint8Array([1]);
      });
      const created = vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:folder");
      vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => {});
      const click = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
      const { result, handle, unmount, queries } = downloadHook();
      await act(async () => result.current.startDownload("/folder", true));
      expect(handle.fail).toHaveBeenCalledWith(expect.stringContaining("unreadable source"));
      expect(handle.finish).not.toHaveBeenCalled();
      expect(created).not.toHaveBeenCalled();
      expect(click).not.toHaveBeenCalled();
      unmount();
      queries.clear();
    },
  );

  it.each(["before listing", "during the last read"] as const)(
    "rejects cancellation %s rather than returning a partial collection",
    async (when) => {
      const client = new ZeroFsClient();
      const abort = new AbortController();
      const listing = vi.spyOn(client, "directoryListing").mockResolvedValue({ entries: [entry("file")], rawNames: [] });
      vi.spyOn(client, "readFile").mockImplementation(async () => {
        abort.abort();
        return new Uint8Array([1]);
      });
      if (when === "before listing") abort.abort();
      await expect(client.collectFiles("/folder", "", () => {}, abort.signal)).rejects.toMatchObject({ name: "AbortError" });
      if (when === "before listing") expect(listing).not.toHaveBeenCalled();
    },
  );

  it("does not download when cancellation arrives as collection finishes", async () => {
    const { result, handle, abort, unmount, queries } = downloadHook();
    vi.spyOn(p9client, "collectFiles").mockImplementation(async () => {
      abort.abort();
      return [{ path: "file", data: new Uint8Array([1]) }];
    });
    const created = vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:folder");
    vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => {});
    vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
    await act(async () => result.current.startDownload("/folder", true));
    expect(created).not.toHaveBeenCalled();
    expect(handle.finish).not.toHaveBeenCalled();
    expect(handle.fail).not.toHaveBeenCalled();
    unmount();
    queries.clear();
  });

  it("preserves prototype-like filenames and their exact contents in the ZIP", async () => {
    const files = ["__proto__", "constructor", "toString", "nested/__proto__"].map((path, index) => ({
      path, data: new Uint8Array([index + 1, 0, 255]),
    }));
    vi.spyOn(p9client, "collectFiles").mockResolvedValue(files);
    const created = vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:folder");
    vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => {});
    const click = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
    const { result, handle, unmount, queries } = downloadHook();
    await act(async () => result.current.startDownload("/folder", true));
    expect(handle.fail).not.toHaveBeenCalled();
    expect(handle.finish).toHaveBeenCalledOnce();
    expect(click).toHaveBeenCalledOnce();
    const blob = created.mock.calls[0][0] as Blob;
    // The streaming reader preserves filename keys too; unzipSync's plain
    // object output cannot represent an own __proto__ property reliably.
    const decoded = new Map<string, number[]>();
    const unzip = new Unzip((file) => {
      const bytes: number[] = [];
      file.ondata = (error, chunk, final) => {
        if (error) throw error;
        bytes.push(...chunk);
        if (final) decoded.set(file.name, bytes);
      };
      file.start();
    });
    unzip.register(UnzipInflate);
    unzip.push(new Uint8Array(await blob.arrayBuffer()), true);
    expect(decoded).toEqual(new Map(files.map(({ path, data }) => [path, [...data]])));
    unmount();
    queries.clear();
  });
});
