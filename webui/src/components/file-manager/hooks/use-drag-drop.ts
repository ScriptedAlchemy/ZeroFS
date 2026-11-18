import { useState } from "react";
import { type FileEntry } from "../../../lib/zerofs/client";
import { joinPath } from "../../../lib/format";
import { DRAG_MIME, draggedPaths } from "../drag-data";

interface UseDragAndDropOptions {
  currentPath: string;
  selectedNames: Set<string>;
  onMoveToDir: (sourcePath: string, targetDir: string) => void;
}

export function useDragAndDrop({ currentPath, selectedNames, onMoveToDir }: UseDragAndDropOptions) {
  const [dropTarget, setDropTarget] = useState<string | null>(null);

  const onDragStart = (e: React.DragEvent, entry: FileEntry) => {
    const names = selectedNames.has(entry.name) && selectedNames.size > 1 ? Array.from(selectedNames) : [entry.name];
    e.dataTransfer.setData(DRAG_MIME, JSON.stringify(names.map((name) => joinPath(currentPath, name))));
    e.dataTransfer.effectAllowed = "move";

    const badge = document.createElement("div");
    badge.textContent = names.length > 1 ? `${names.length} items` : entry.name;
    Object.assign(badge.style, {
      position: "absolute",
      top: "-9999px",
      padding: "6px 12px",
      borderRadius: "6px",
      background: "var(--color-card)",
      color: "var(--color-foreground)",
      fontSize: "13px",
      fontWeight: "500",
      whiteSpace: "nowrap",
      maxWidth: "300px",
      overflow: "hidden",
      textOverflow: "ellipsis",
    });
    document.body.appendChild(badge);
    e.dataTransfer.setDragImage(badge, badge.offsetWidth / 2, badge.offsetHeight / 2);
    requestAnimationFrame(() => badge.remove());
  };

  const onDirDragOver = (e: React.DragEvent, entry: FileEntry) => {
    if (!entry.resolvedIsDir || !e.dataTransfer.types.includes(DRAG_MIME)) return;
    e.preventDefault();
    e.dataTransfer.dropEffect = "move";
    setDropTarget(entry.name);
  };

  const onDirDragLeave = (_e: React.DragEvent, entry: FileEntry) => {
    setDropTarget((prev) => (prev === entry.name ? null : prev));
  };

  const onDirDrop = (e: React.DragEvent, entry: FileEntry) => {
    e.preventDefault();
    setDropTarget(null);
    if (!entry.resolvedIsDir) return;
    const targetDir = joinPath(currentPath, entry.name);
    for (const sourcePath of draggedPaths(e.dataTransfer)) {
      if (sourcePath === targetDir) continue;
      onMoveToDir(sourcePath, targetDir);
    }
  };

  return { dropTarget, onDragStart, onDirDragOver, onDirDragLeave, onDirDrop };
}
