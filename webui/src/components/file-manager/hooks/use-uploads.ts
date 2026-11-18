import { useState, useCallback, useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { useUpload } from "../../../hooks/use-ninep";
import { isErrno, p9client } from "../../../lib/zerofs/client";
import { useFileTransfers } from "../../transfers/use-file-transfers";
import type { TransferHandle } from "../../transfers/TransferContext";
import { formatSize, joinPath } from "../../../lib/format";
import { formatError } from "../../../lib/errors";
import { pooled } from "../../../lib/async";
import { readDroppedItems, getDirectoriesToCreate } from "../../../lib/folder-upload";

export interface OverwritePrompt {
  existing: string[];
  resolve: (action: "overwrite" | "skip" | "cancel") => void;
}

export function useUploads(path: string) {
  const qc = useQueryClient();
  const transfers = useFileTransfers();
  const uploadMut = useUpload();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [overwritePrompts, setOverwritePrompts] = useState<OverwritePrompt[]>([]);

  const chooseUploadTargets = useCallback(
    async (names: string[], signal?: AbortSignal) => {
      const targets = new Map(names.map((name) => [name, false]));
      const existing = new Set<string>();
      await pooled([...targets.keys()], 8, async (name) => {
        if (signal?.aborted) return;
        try {
          await p9client.stat(joinPath(path, name));
          existing.add(name);
        } catch (error) {
          if (!isErrno(error, 2)) throw error;
        }
      });
      if (signal?.aborted) return new Map<string, boolean>();
      if (existing.size === 0) return targets;
      const action = await new Promise<"overwrite" | "skip" | "cancel">((resolve) => {
        const prompt: OverwritePrompt = {
          existing: [...targets.keys()].filter((name) => existing.has(name)),
          resolve: (answer) => {
            signal?.removeEventListener("abort", cancel);
            setOverwritePrompts((queue) => queue.filter((item) => item !== prompt));
            resolve(answer);
          },
        };
        const cancel = () => prompt.resolve("cancel");
        signal?.addEventListener("abort", cancel, { once: true });
        setOverwritePrompts((queue) => [...queue, prompt]);
      });
      if (action === "cancel") return new Map<string, boolean>();
      for (const name of existing) {
        if (action === "overwrite") targets.set(name, true);
        else targets.delete(name);
      }
      return targets;
    },
    [path],
  );

  const startDownload = useCallback(
    async (filePath: string, isDir = false) => {
      const name = filePath.split("/").pop() ?? "download";
      const retry = () => startDownload(filePath, isDir);

      if (isDir) {
        const handle = transfers.startDownload(`${name}.zip`, retry);
        try {
          let collected = 0;
          let totalBytes = 0;
          const files = await p9client.collectFiles(
            filePath,
            "",
            (f) => {
              collected++;
              handle.update(0, 0, `${collected} files found: ${f}`);
            },
            handle.signal,
          );
          totalBytes = files.reduce((sum, f) => sum + f.data.length, 0);
          handle.update(0, 0, `Zipping ${collected} files (${formatSize(totalBytes)})...`);
          const { zipSync } = await import("fflate");
          const zipData: Record<string, Uint8Array> = {};
          for (const f of files) zipData[f.path] = f.data;
          const zipped = zipSync(zipData);
          const blob = new Blob([new Uint8Array(zipped)], { type: "application/zip" });
          const url = URL.createObjectURL(blob);
          const a = document.createElement("a");
          a.href = url;
          a.download = `${name}.zip`;
          a.click();
          URL.revokeObjectURL(url);
          handle.update(zipped.length, zipped.length);
          handle.finish();
        } catch (err) {
          if (err instanceof DOMException && err.name === "AbortError") return;
          handle.fail(formatError(err));
        }
      } else {
        const handle = transfers.startDownload(name, retry);
        try {
          await p9client.download(
            filePath,
            (received, total) => {
              handle.update(received, total);
            },
            handle.signal,
          );
          handle.finish();
        } catch (err) {
          if (err instanceof DOMException && err.name === "AbortError") return;
          handle.fail(formatError(err));
        }
      }
    },
    [transfers],
  );

  const deleteEntry = useCallback(
    async (
      entryName: string,
      isDir: boolean,
      removeMut: { mutateAsync: (args: { path: string; isDir: boolean }) => Promise<unknown> },
    ) => {
      const entryPath = joinPath(path, entryName);
      if (isDir) {
        const doDelete = async () => {
          const retry = () => doDelete();
          const handle = transfers.startDelete(entryName, retry);
          try {
            const count = await p9client.removeDirRecursive(
              entryPath,
              (deleted, current) => handle.update(deleted, 0, current.split("/").pop()),
              handle.signal,
            );
            handle.update(count, count);
            handle.finish();
          } catch (err) {
            if (err instanceof DOMException && err.name === "AbortError") return;
            handle.fail(formatError(err));
          }
        };
        await doDelete();
      } else {
        try {
          await removeMut.mutateAsync({ path: entryPath, isDir: false });
          toast.success(`Deleted ${entryName}`);
        } catch (err) {
          toast.error(`Failed to delete ${entryName}`, { description: formatError(err) });
        }
      }
    },
    [path, transfers],
  );

  const doUpload = useCallback(
    async (filesToUpload: File[], targets: Map<string, boolean>) => {
      const batch = p9client.beginUploadBatch();
      const completed: TransferHandle[] = [];
      await pooled(filesToUpload, 8, async (file) => {
        const uploadOne = async (mode: "batch" | "standalone") => {
          const retry = () => uploadOne("standalone");
          const uploadBatch = mode === "batch" ? batch : undefined;
          const handle = transfers.startUpload(file.name, file.size, retry);
          try {
            await uploadMut.mutateAsync({
              dirPath: path,
              file,
              onProgress: (sent, total) => handle.update(sent, total),
              signal: handle.signal,
              batch: uploadBatch,
              replace: targets.get(file.name),
            });
            if (uploadBatch) completed.push(handle);
            else handle.finish();
          } catch (err) {
            if (err instanceof DOMException && err.name === "AbortError") return;
            handle.fail(formatError(err));
          }
        };
        await uploadOne("batch");
      });

      if (completed.length === 0) return;
      try {
        await p9client.finishUploadBatch(batch);
        for (const handle of completed) {
          if (!handle.signal.aborted) handle.finish();
        }
      } catch (err) {
        const message = formatError(err);
        for (const handle of completed) handle.fail(message);
      }
    },
    [path, uploadMut, transfers],
  );

  const uploadFiles = useCallback(
    async (files: FileList) => {
      const allFiles = Array.from(files);
      try {
        const targets = await chooseUploadTargets(allFiles.map((file) => file.name));
        await doUpload(
          allFiles.filter((file) => targets.has(file.name)),
          targets,
        );
      } catch (err) {
        toast.error("Failed to check upload destinations", { description: formatError(err) });
      }
    },
    [chooseUploadTargets, doUpload],
  );

  const handleFileUpload = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      if (e.target.files) uploadFiles(e.target.files);
      e.target.value = "";
    },
    [uploadFiles],
  );

  const handleFolderUpload = useCallback(
    async (items: DataTransferItemList) => {
      try {
        const handle = transfers.startUpload("Scanning folder...", 0);
        const found = await readDroppedItems(
          items,
          (progress) => {
            handle.update(0, 0, `${progress.filesFound} files found: ${progress.currentPath}`);
          },
          handle.signal,
        );

        if (handle.signal.aborted) return;

        if (found.length === 0) {
          handle.finish();
          toast.info("No files found in folder");
          return;
        }

        let targets: Map<string, boolean>;
        try {
          targets = await chooseUploadTargets(
            found.map((item) => item.relativePath),
            handle.signal,
          );
        } catch (err) {
          handle.fail(formatError(err));
          return;
        }
        if (handle.signal.aborted) return;
        const uploadItems = found.filter((item) => targets.has(item.relativePath));
        if (uploadItems.length === 0) {
          handle.finish();
          return;
        }
        const totalBytes = uploadItems.reduce((sum, item) => sum + item.file.size, 0);
        handle.setName(`${uploadItems.length} files`);
        handle.update(0, totalBytes);
        let bytesUploaded = 0;
        let errors = 0;

        const dirs = getDirectoriesToCreate(uploadItems);
        let created = 0;
        const byDepth = new Map<number, string[]>();
        for (const dir of dirs) {
          const depth = dir.split("/").length;
          if (!byDepth.has(depth)) byDepth.set(depth, []);
          byDepth.get(depth)!.push(dir);
        }
        for (const depth of [...byDepth.keys()].sort((a, b) => a - b)) {
          if (handle.signal.aborted) break;
          const batch = byDepth.get(depth)!;
          await pooled(batch, 16, async (dir) => {
            if (handle.signal.aborted) return;
            const fullDir = joinPath(path, dir);
            try {
              await p9client.mkdir(fullDir);
            } catch {
              /* may already exist */
            }
            created++;
            handle.update(0, totalBytes, `Creating folders ${created}/${dirs.length}`);
          });
        }

        let filesUploaded = 0;
        const batch = p9client.beginUploadBatch();
        await pooled(uploadItems, 16, async (item) => {
          if (handle.signal.aborted) return;
          const targetPath = joinPath(path, item.relativePath);
          try {
            await p9client.uploadBlob(targetPath, item.file, {
              replace: targets.get(item.relativePath),
              onProgress: (sent) => {
                handle.update(bytesUploaded + sent, totalBytes, `${filesUploaded}/${uploadItems.length} files`);
              },
              signal: handle.signal,
              batch,
            });
            bytesUploaded += item.file.size;
          } catch (err) {
            if (err instanceof DOMException && err.name === "AbortError") return;
            errors++;
            toast.error(`Failed to upload ${item.relativePath}`, { description: formatError(err) });
          }
          filesUploaded++;
          handle.update(bytesUploaded, totalBytes, `${filesUploaded}/${uploadItems.length} files`);
        });

        if (errors < uploadItems.length) {
          try {
            await p9client.finishUploadBatch(batch);
          } catch (err) {
            if (!handle.signal.aborted) handle.fail(formatError(err));
            return;
          }
        }

        if (handle.signal.aborted) return;
        if (errors === uploadItems.length) {
          handle.fail(`All ${errors} uploads failed`);
        } else {
          handle.finish();
          if (errors > 0) toast.warning(`${errors} file${errors > 1 ? "s" : ""} failed to upload`);
        }
        qc.invalidateQueries({ queryKey: ["9p", "ls"] });
      } catch (err) {
        toast.error("Failed to read dropped items", { description: formatError(err) });
      }
    },
    [path, qc, transfers, chooseUploadTargets],
  );

  return {
    startDownload,
    deleteEntry,
    uploadFiles,
    handleFileUpload,
    handleFolderUpload,
    fileInputRef,
    overwritePrompt: overwritePrompts[0] ?? null,
  };
}
