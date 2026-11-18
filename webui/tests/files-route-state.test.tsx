import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Link, MemoryRouter, useLocation, useNavigate } from "react-router";
import { afterEach, expect, it, vi } from "vitest";
import { FilesPage } from "../src/pages/files";
import { ActivePage } from "../src/hooks/use-title";

const visits = vi.hoisted(() => vi.fn());
vi.mock("../src/components/file-manager/FileManager", () => ({
  FileManager: ({ path }: { path: string }) => {
    visits(path);
    return <output aria-label="File manager directory">{path}</output>;
  },
}));
vi.mock("../src/lib/zerofs/client", () => ({ p9client: {} }));

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

function KeptAliveFiles() {
  const location = useLocation();
  const navigate = useNavigate();
  return (
    <>
      <Link to="/storage">Storage</Link>
      <button onClick={() => navigate(-1)}>Back</button>
      <Link to="/files/other">Other directory</Link>
      <ActivePage value={location.pathname.startsWith("/files")}>
        <FilesPage />
      </ActivePage>
    </>
  );
}

it("keeps an inactive file tab on its directory while operator routes change", async () => {
  const queries = new QueryClient();
  render(
    <QueryClientProvider client={queries}>
      <MemoryRouter initialEntries={["/files/photos"]}>
        <KeptAliveFiles />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  expect(screen.getByLabelText("File manager directory").textContent).toBe("/photos");
  fireEvent.click(screen.getByRole("link", { name: "Storage" }));
  expect(screen.getByLabelText("File manager directory").textContent).toBe("/photos");
  expect(visits.mock.calls.flat()).not.toContain("/storage");
  fireEvent.click(screen.getByRole("button", { name: "Back" }));
  await waitFor(() => expect(screen.getByLabelText("File manager directory").textContent).toBe("/photos"));
  fireEvent.click(screen.getByRole("link", { name: "Other directory" }));
  await waitFor(() => expect(screen.getByLabelText("File manager directory").textContent).toBe("/other"));
  queries.clear();
});
