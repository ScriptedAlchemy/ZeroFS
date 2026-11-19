import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("../src/lib/grpc/client", () => ({
  adminClient: {
    getPoolStatus: () => Promise.resolve({ json: "" }),
    streamStats: () => (async function* () {})(),
  },
}));

import { StoragePage } from "../src/pages/storage";

const materialized = { write_ack_mode: "materialized" };
const volatile = {
  write_ack_mode: "volatile_memory",
  volatile_memory_gb: 0.268435456,
  volatile_max_operations: 2048,
};
const status = {
  active: materialized,
  desired: materialized,
  activation: "restart",
  pending_restart: false,
  editable: true,
};
const fetchMock = vi.fn<typeof fetch>();
const section = () => within(screen.getByRole("region", { name: "Write acknowledgement" }));

beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
  vi.stubGlobal("isSecureContext", true);
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  fetchMock.mockReset();
});

async function edit() {
  fetchMock.mockResolvedValueOnce(Response.json(status));
  render(<StoragePage />);
  await section().findByRole("button", { name: "Save for restart" });
}

function setToken(value: string): HTMLInputElement {
  const input = section().getByLabelText<HTMLInputElement>("Admin token");
  fireEvent.change(input, { target: { value } });
  return input;
}

function submit() {
  fireEvent.click(section().getByRole("button", { name: "Save for restart" }));
}

function selectVolatile() {
  fireEvent.change(section().getByLabelText("Desired mode"), { target: { value: "volatile_memory" } });
}

describe("Storage write acknowledgement settings", () => {
  it("loads without authorization and saves the desired geometry without changing the active mode", async () => {
    await edit();
    expect(fetchMock).toHaveBeenNthCalledWith(1, "/api/settings/write-ack", {
      signal: expect.any(AbortSignal),
      cache: "no-store",
      credentials: "omit",
      redirect: "error",
    });
    expect(section().getByText(/still in RAM/).textContent).toContain("fsync and protocol durability requests");
    selectVolatile();
    const token = setToken("admin-for-one-save");
    fetchMock.mockResolvedValueOnce(Response.json({ ...status, desired: volatile, pending_restart: true }));
    submit();
    expect(token.value).toBe("");
    expect(fetchMock).toHaveBeenNthCalledWith(2, "/api/settings/write-ack", {
      method: "PUT",
      headers: { Authorization: "Bearer admin-for-one-save", "Content-Type": "application/json" },
      cache: "no-store",
      credentials: "omit",
      redirect: "error",
      body: JSON.stringify({ version: 1, filesystem: volatile }),
    });
    expect(
      await section().findByText("Settings saved. Restart required to activate the desired settings."),
    ).toBeTruthy();
    expect(section().getByText("Active").nextElementSibling?.textContent).toBe("Materialized");
    expect(section().getByText("Desired").nextElementSibling?.textContent).toBe(
      "Volatile memory · 256 MiB · 2,048 operations",
    );
    expect(section().getByLabelText<HTMLInputElement>("RAM limit (MiB)").value).toBe("256");
  });

  it("rejects fractional and out-of-range geometry without sending the token", async () => {
    await edit();
    selectVolatile();
    const memory = section().getByLabelText("RAM limit (MiB)");
    const operations = section().getByLabelText("Pending operation limit");
    for (const [mib, operationLimit] of [
      ["15", "1"],
      ["257", "1"],
      ["16.5", "1"],
      ["16", "0"],
      ["16", "2049"],
    ]) {
      fireEvent.change(memory, { target: { value: mib } });
      fireEvent.change(operations, { target: { value: operationLimit } });
      const token = setToken("never-send");
      submit();
      expect(await section().findByRole("alert")).toBeTruthy();
      expect(token.value).toBe("");
      expect(fetchMock).toHaveBeenCalledTimes(1);
    }
    fireEvent.change(operations, { target: { value: "1" } });
    setToken("valid");
    fetchMock.mockResolvedValueOnce(Response.json(status));
    submit();
    const body = fetchMock.mock.calls[1][1]?.body;
    expect(JSON.parse(String(body)).filesystem).toEqual({
      write_ack_mode: "volatile_memory",
      volatile_memory_gb: 0.016777216,
      volatile_max_operations: 1,
    });
    await section().findByText(/Settings saved/);
  });

  it("round-trips an integer MiB limit represented in decimal GB", async () => {
    const desired = { ...volatile, volatile_memory_gb: 0.03145728 };
    const pending = { ...status, desired, pending_restart: true };
    fetchMock.mockResolvedValueOnce(Response.json(pending)).mockResolvedValueOnce(Response.json(pending));
    render(<StoragePage />);
    expect((await section().findByLabelText<HTMLInputElement>("RAM limit (MiB)")).value).toBe("30");
    setToken("admin");
    submit();
    expect(fetchMock.mock.calls[1][1]?.body).toBe(JSON.stringify({ version: 1, filesystem: desired }));
    await section().findByText(/Settings saved/);
  });

  it("clears rejected credentials and sends only the materialized mode on retry", async () => {
    await edit();
    const token = setToken("rejected-secret");
    fetchMock.mockResolvedValueOnce(new Response("rejected-secret", { status: 401 }));
    submit();
    expect((await section().findByRole("alert")).textContent).toContain("refused authorization");
    expect(token.value).toBe("");
    expect(section().queryByText(/rejected-secret/)).toBeNull();
    submit();
    expect((await section().findByRole("alert")).textContent).toBe("Enter an admin token to save.");
    expect(fetchMock).toHaveBeenCalledTimes(2);
    setToken("fresh");
    fetchMock.mockResolvedValueOnce(Response.json(status));
    submit();
    expect(fetchMock.mock.calls[2][1]?.body).toBe(JSON.stringify({ version: 1, filesystem: materialized }));
    await section().findByText(/Settings saved/);
  });

  it.each(["insecure browser", "read-only engine"])("shows status and hides the token form in a %s", async (reason) => {
    vi.stubGlobal("isSecureContext", reason !== "insecure browser");
    fetchMock.mockResolvedValueOnce(Response.json({ ...status, editable: reason !== "read-only engine" }));
    render(<StoragePage />);
    await section().findByText(reason === "insecure browser" ? /over HTTPS/ : /read-only/);
    expect(section().queryByLabelText("Admin token")).toBeNull();
    expect(section().queryByRole("button", { name: "Save for restart" })).toBeNull();
  });

  it.each([
    { name: "missing route", response: () => new Response(null, { status: 404 }) },
    {
      name: "HTML application fallback",
      response: () =>
        new Response("<!doctype html><html><body><div id=\"root\"></div></body></html>", {
          headers: { "Content-Type": "text/html; charset=utf-8" },
        }),
    },
  ])("hides the section when running against an older engine ($name)", async ({ response }) => {
    fetchMock.mockResolvedValueOnce(response());
    render(<StoragePage />);
    expect(screen.getByRole("heading", { name: "Storage" })).toBeTruthy();
    await waitFor(() => expect(screen.queryByRole("region", { name: "Write acknowledgement" })).toBeNull());
  });

  it.each([
    {
      name: "server error",
      load: () =>
        Promise.resolve(new Response("Service unavailable", { status: 503, headers: { "Content-Type": "text/html" } })),
    },
    { name: "invalid JSON status", load: () => Promise.resolve(Response.json({ error: "unavailable" })) },
    { name: "network failure", load: () => Promise.reject(new TypeError("Network request failed")) },
  ])("reports a load error and allows a retry ($name)", async ({ load }) => {
    fetchMock.mockImplementationOnce(load).mockResolvedValueOnce(Response.json(status));
    render(<StoragePage />);
    expect((await section().findByRole("alert")).textContent).toMatch(/Could not load/);
    fireEvent.click(section().getByRole("button", { name: "Retry" }));
    expect(await section().findByRole("button", { name: "Save for restart" })).toBeTruthy();
  });
});
