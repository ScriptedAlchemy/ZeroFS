import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { afterEach, expect, it, vi } from "vitest";
import { toast } from "sonner";
import { TransferPanel } from "../src/components/transfers/TransferPanel";
import { useUploads } from "../src/components/file-manager/hooks/use-uploads";
import { ActivePage } from "../src/hooks/use-title";
import { p9client } from "../src/lib/zerofs/client";

vi.mock("../src/generated/zerofs-client/zerofs_client", () => ({ default: vi.fn(), Client: {} }));

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

function UploadFolder({ path, files }: { path: string; files: FileList }) {
  const uploads = useUploads(path);
  return (
    <>
      <button onClick={() => void uploads.uploadFiles(files)}>Upload</button>
      {uploads.overwritePrompt && (
        <div>
          <p>Confirm replacement</p>
          <button onClick={() => uploads.overwritePrompt!.resolve("overwrite")}>Overwrite</button>
          <button onClick={() => uploads.overwritePrompt!.resolve("cancel")}>Cancel overwrite</button>
        </div>
      )}
    </>
  );
}

it("keeps a blocked retry visible and never waits for an overwrite prompt on a hidden Files page", async () => {
  const queries = new QueryClient();
  const transfer = new DataTransfer();
  transfer.items.add(new File(["original"], "Report"));
  const stat = vi.spyOn(p9client, "stat").mockRejectedValue(Object.assign(new Error("not found"), { ecode: 2 }));
  const upload = vi.spyOn(p9client, "uploadBlob").mockResolvedValue(undefined);
  vi.spyOn(p9client, "finishUploadBatch").mockRejectedValue(new Error("durability barrier failed"));
  const feedback = vi.spyOn(toast, "error");
  const tree = (active: boolean, path = "/photos") => (
    <QueryClientProvider client={queries}>
      <TransferPanel>
        <ActivePage value={active}>
          <div hidden={!active}>
            <UploadFolder path={path} files={transfer.files} />
          </div>
        </ActivePage>
      </TransferPanel>
    </QueryClientProvider>
  );
  const { rerender } = render(tree(true));
  fireEvent.click(screen.getByRole("button", { name: "Upload", exact: true }));
  await screen.findByText("durability barrier failed");
  stat.mockResolvedValue({} as Awaited<ReturnType<typeof p9client.stat>>);

  // Storage/Monitor keep Files mounted but hidden while the transfer panel is global.
  rerender(tree(false));
  fireEvent.click(screen.getByTitle("Retry"));
  await waitFor(() => expect(feedback).toHaveBeenCalledWith("Open the upload folder to retry", expect.objectContaining({
    description: expect.stringContaining("/photos"),
  })));
  expect(screen.getByText("durability barrier failed")).toBeTruthy();
  expect(screen.queryByText("Confirm replacement")).toBeNull();
  expect(upload).toHaveBeenCalledOnce();

  // A different folder also cannot host the original destination's decision.
  rerender(tree(true, "/other"));
  fireEvent.click(screen.getByTitle("Retry"));
  await waitFor(() => expect(feedback).toHaveBeenCalledTimes(2));
  expect(screen.getByText("durability barrier failed")).toBeTruthy();
  expect(screen.queryByText("Confirm replacement")).toBeNull();

  rerender(tree(true));
  fireEvent.click(screen.getByTitle("Retry"));
  await screen.findByText("Confirm replacement");
  expect((screen.getByTitle("Retry") as HTMLButtonElement).disabled).toBe(true);
  // Navigating away while deciding cancels the pending retry without dropping its error entry.
  rerender(tree(false));
  await waitFor(() => expect((screen.getByTitle("Retry") as HTMLButtonElement).disabled).toBe(false));
  expect(screen.queryByText("Confirm replacement")).toBeNull();
  expect(screen.getByText("durability barrier failed")).toBeTruthy();
  expect(upload).toHaveBeenCalledOnce();

  rerender(tree(true));
  fireEvent.click(screen.getByTitle("Retry"));
  await screen.findByRole("button", { name: "Overwrite", exact: true });
  fireEvent.click(screen.getByRole("button", { name: "Overwrite", exact: true }));
  await waitFor(() => expect(upload).toHaveBeenCalledTimes(2));
  expect(upload.mock.calls[1][0]).toBe("/photos/Report");
  expect(upload.mock.calls[1][2]).toMatchObject({ replace: true, batch: undefined });
  await waitFor(() => expect(screen.queryByText("durability barrier failed")).toBeNull());
  queries.clear();
});
