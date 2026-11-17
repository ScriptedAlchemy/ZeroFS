import { describe, expect, it, vi } from "vitest";
import { ZeroFsClient, type FileEntry } from "../src/lib/zerofs/client";
import { RawFilenameError } from "../src/lib/zerofs/directory-listing";

vi.mock("../src/generated/zerofs-client/zerofs_client", () => ({ default: vi.fn(), Client: {} }));

const entry = (name: string, isDir = false): FileEntry => ({
  name, isDir, resolvedIsDir: isDir, isSymlink: false, size: 1n, mtimeSec: 0n, mode: 0,
  uid: 0, gid: 0, nlink: 1n,
});

describe("browser traversal alongside a lossy filename", () => {
  it.each(["remove", "attributes", "archive", "size", "search"] as const)(
    "%s reports the raw bytes and never addresses the text sibling",
    async (operation) => {
      const client = new ZeroFsClient();
      const listing = vi.spyOn(client, "directoryListing").mockImplementation(async (path) => path === "/"
        ? { entries: [entry("nested", true)], rawNames: [] }
        : { entries: [entry("�")], rawNames: ["ff"] });
      const remove = vi.spyOn(client, "remove").mockResolvedValue();
      const removeDir = vi.spyOn(client, "removeDir").mockResolvedValue();
      const setattr = vi.spyOn(client, "setattr").mockResolvedValue();
      const readFile = vi.spyOn(client, "readFile").mockResolvedValue(new Uint8Array());
      const result = vi.fn();
      const action = {
        remove: () => client.removeDirRecursive("/"),
        attributes: () => client.setattrRecursive("/", { mode: 0o600 }),
        archive: () => client.collectFiles("/", "", result),
        size: () => client.dirSize("/"),
        search: () => client.search("/", "�", result),
      }[operation];
      await expect(action()).rejects.toBeInstanceOf(RawFilenameError);
      expect(listing.mock.calls.map(([path]) => path)).toEqual(["/", "/nested"]);
      expect(remove).not.toHaveBeenCalled();
      expect(removeDir).not.toHaveBeenCalled();
      expect(setattr.mock.calls.map(([path]) => path)).not.toContain("/nested/�");
      expect(readFile).not.toHaveBeenCalled();
      expect(result).not.toHaveBeenCalled();
    },
  );
});
