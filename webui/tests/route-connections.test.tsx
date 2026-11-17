import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, expect, it, vi } from "vitest";
import App from "../src/App";
import { useTitle } from "../src/hooks/use-title";
import { useNinePVisualState } from "../src/hooks/use-ninep-connection";

vi.mock("../src/hooks/use-ninep-connection", () => ({ useNinePVisualState: vi.fn(() => "disconnected") }));
vi.mock("../src/pages/dashboard", () => ({ DashboardPage: () => <TestPage title="Monitor" /> }));
vi.mock("../src/pages/storage", () => ({ StoragePage: () => <TestPage title="Storage" /> }));
vi.mock("../src/pages/files", () => ({ FilesPage: () => <h1>Files</h1> }));
vi.mock("../src/pages/terminal", () => ({ TerminalPage: () => <h1>Terminal</h1> }));

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

it.each([
  ["/dashboard", "Monitor"],
  ["/storage", "Storage"],
])("opens %s without a 9P connection", async (path, title) => {
  render(
    <MemoryRouter initialEntries={[path]}>
      <App />
    </MemoryRouter>,
  );
  await screen.findByRole("heading", { name: title });
  expect(useNinePVisualState).not.toHaveBeenCalled();
  expect(screen.queryByText("Connection lost")).toBeNull();
});

it.each(["/files", "/terminal"])("shows the disconnected file transport on %s", async (path) => {
  render(
    <MemoryRouter initialEntries={[path]}>
      <App />
    </MemoryRouter>,
  );
  await screen.findByText("Connection lost");
});

function TestPage({ title }: { title: string }) {
  useTitle(title);
  return <h1>{title}</h1>;
}

it("restores the title when returning to a mounted page", async () => {
  render(
    <MemoryRouter initialEntries={["/storage"]}>
      <App />
    </MemoryRouter>,
  );
  await waitFor(() => expect(document.title).toBe("ZeroFS - Storage"));
  fireEvent.click(screen.getByRole("link", { name: "Monitor" }));
  await waitFor(() => expect(document.title).toBe("ZeroFS - Monitor"));
  fireEvent.click(screen.getByRole("link", { name: "Storage" }));
  await waitFor(() => expect(document.title).toBe("ZeroFS - Storage"));
});
