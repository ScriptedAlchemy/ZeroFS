export const DRAG_MIME = "application/x-zerofs-entry";

/** Drag payloads carry absolute source paths; the active tab can change before drop. */
export function draggedPaths(transfer: Pick<DataTransfer, "getData">): string[] {
  try {
    const paths: unknown = JSON.parse(transfer.getData(DRAG_MIME));
    return Array.isArray(paths) && paths.every((path) =>
      typeof path === "string" && path.startsWith("/") && !path.endsWith("/") && !path.includes("\0") &&
      path.split("/").every((part) => part !== "." && part !== ".."),
    ) ? paths : [];
  } catch {
    return [];
  }
}
