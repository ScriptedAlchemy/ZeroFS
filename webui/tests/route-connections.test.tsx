import { cleanup, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, expect, it, vi } from "vitest";
import App from "../src/App";
import { useNinePVisualState } from "../src/hooks/use-ninep-connection";

vi.mock("../src/hooks/use-ninep-connection", () => ({ useNinePVisualState: vi.fn(() => "disconnected") }));
vi.mock("../src/pages/dashboard", () => ({ DashboardPage: () => <h1>Monitor</h1> }));
vi.mock("../src/pages/storage", () => ({ StoragePage: () => <h1>Storage</h1> }));
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
