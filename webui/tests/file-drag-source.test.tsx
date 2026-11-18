import { act, cleanup, fireEvent, render, renderHook, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MemoryRouter } from "react-router";
import type { DragEvent } from "react";
import { afterEach, expect, it, vi } from "vitest";
import { Breadcrumb } from "../src/components/file-manager/Breadcrumb";
import { DRAG_MIME } from "../src/components/file-manager/FileBrowser";
import { useDragAndDrop } from "../src/components/file-manager/hooks/use-drag-drop";
import { FilesPage } from "../src/pages/files";
import { p9client, type FileEntry } from "../src/lib/zerofs/client";

const file: FileEntry = {
  name: "report", isDir: false, resolvedIsDir: false, isSymlink: false, size: 1n, mtimeSec: 0n,
  mode: 0, uid: 0, gid: 0, nlink: 1n,
};

vi.mock("../src/lib/zerofs/client", () => ({ p9client: { rename: vi.fn() } }));
vi.mock("../src/components/file-manager/FileManager", () => ({
  FileManager: ({ path, onNavigate }: { path: string; onNavigate: (path: string) => void }) => {
    const drag = useDragAndDrop({ currentPath: path, selectedNames: new Set(), onMoveToDir: vi.fn() });
    return <>
      <output aria-label="Current directory">{path}</output>
      <button onClick={() => onNavigate("/target")}>Open target</button>
      <button draggable onDragStart={(event) => drag.onDragStart(event, file)}>Drag report</button>
    </>;
  },
}));

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.restoreAllMocks();
});

function transferEvent(transfer: DataTransfer): DragEvent {
  return { dataTransfer: transfer, preventDefault: vi.fn() } as unknown as DragEvent;
}

function sourceTransfer() {
  const transfer = new DataTransfer();
  transfer.setDragImage = vi.fn();
  return transfer;
}

it("keeps the original directory when a drag enters a child after the view changes", () => {
  const move = vi.fn();
  const { result, rerender } = renderHook(({ path }) => useDragAndDrop({
    currentPath: path, selectedNames: new Set(["report", "other"]), onMoveToDir: move,
  }), { initialProps: { path: "/source" } });
  const transfer = sourceTransfer();
  act(() => result.current.onDragStart(transferEvent(transfer), file));
  rerender({ path: "/target" });
  act(() => result.current.onDirDrop(transferEvent(transfer), { ...file, name: "child", resolvedIsDir: true }));
  expect(move.mock.calls).toEqual([["/source/report", "/target/child"], ["/source/other", "/target/child"]]);
});

it("uses original source paths for breadcrumb drops", () => {
  const move = vi.fn();
  const { result } = renderHook(() => useDragAndDrop({ currentPath: "/source", selectedNames: new Set(), onMoveToDir: move }));
  const transfer = sourceTransfer();
  act(() => result.current.onDragStart(transferEvent(transfer), file));
  render(<Breadcrumb path="/target/child" onNavigate={vi.fn()} onMoveToDir={move} />);
  fireEvent.drop(screen.getByRole("button", { name: "target", exact: true }), { dataTransfer: transfer });
  expect(move).toHaveBeenCalledWith("/source/report", "/target");
});

it("moves the original file after hovering long enough to switch tabs", async () => {
  vi.useFakeTimers();
  vi.mocked(p9client.rename).mockResolvedValue(undefined);
  const queries = new QueryClient();
  const { container } = render(<QueryClientProvider client={queries}>
    <MemoryRouter initialEntries={["/files/source"]}><FilesPage /></MemoryRouter>
  </QueryClientProvider>);
  fireEvent.click(container.querySelector("svg.lucide-plus")!.closest("button")!);
  fireEvent.click(screen.getByRole("button", { name: "Open target" }));
  fireEvent.click(screen.getByText("source", { exact: true }).closest("button")!);
  const transfer = sourceTransfer();
  fireEvent.dragStart(screen.getByRole("button", { name: "Drag report" }), { dataTransfer: transfer });
  const target = screen.getByText("target", { exact: true }).closest("button")!;
  fireEvent.dragOver(target, { dataTransfer: transfer });
  // Browsers repeat dragover while hovering. These events must not keep
  // postponing the same tab's 600 ms switch deadline.
  for (let i = 0; i < 2; i++) {
    act(() => vi.advanceTimersByTime(200));
    fireEvent.dragOver(target, { dataTransfer: transfer });
  }
  act(() => vi.advanceTimersByTime(250));
  expect(screen.getByLabelText("Current directory").textContent).toBe("/target");
  await act(async () => fireEvent.drop(target, { dataTransfer: transfer }));
  expect(p9client.rename).toHaveBeenCalledWith("/source/report", "/target/report");
  queries.clear();
});

it("ignores malformed or unanchored drag data without moving a current-directory file", () => {
  const move = vi.fn();
  const { result } = renderHook(() => useDragAndDrop({ currentPath: "/target", selectedNames: new Set(), onMoveToDir: move }));
  for (const raw of ["not JSON", JSON.stringify(["report"]), JSON.stringify(["/source/../report"])]) {
    const transfer = sourceTransfer();
    transfer.setData(DRAG_MIME, raw);
    act(() => expect(() => result.current.onDirDrop(transferEvent(transfer), { ...file, name: "child", resolvedIsDir: true })).not.toThrow());
  }
  expect(move).not.toHaveBeenCalled();
});
