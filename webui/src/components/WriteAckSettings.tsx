import { useEffect, useRef, useState } from "react";
import type { FormEvent } from "react";
import { Button } from "./ui/Button";

const endpoint = "/api/settings/write-ack";
const bytesPerMiB = 1024 ** 2;
const bytesPerGb = 1_000_000_000;
const qualifiedMiB = 256;
const qualifiedOperations = 2048;

type FilesystemSettings =
  | { write_ack_mode: "materialized" }
  | { write_ack_mode: "volatile_memory"; volatile_memory_gb: number; volatile_max_operations: number };

type Status = {
  active: FilesystemSettings;
  desired: FilesystemSettings;
  activation: "restart";
  pending_restart: boolean;
  editable: boolean;
};

type Load = { kind: "loading" | "unsupported" | "error" } | { kind: "ready"; status: Status };

function parseFilesystem(value: unknown): FilesystemSettings {
  const fields = value as Partial<FilesystemSettings> | null;
  if (fields?.write_ack_mode === "materialized") return { write_ack_mode: "materialized" };
  if (
    fields?.write_ack_mode === "volatile_memory" &&
    typeof fields.volatile_memory_gb === "number" &&
    Number.isFinite(fields.volatile_memory_gb) &&
    fields.volatile_memory_gb > 0 &&
    typeof fields.volatile_max_operations === "number" &&
    Number.isSafeInteger(fields.volatile_max_operations) &&
    fields.volatile_max_operations > 0
  ) {
    return {
      write_ack_mode: fields.write_ack_mode,
      volatile_memory_gb: fields.volatile_memory_gb,
      volatile_max_operations: fields.volatile_max_operations,
    };
  }
  throw new Error("Invalid write acknowledgement settings");
}

async function readStatus(response: Response): Promise<Status> {
  const value = (await response.json()) as Partial<Status> | null;
  if (
    value?.activation !== "restart" ||
    typeof value.pending_restart !== "boolean" ||
    typeof value.editable !== "boolean"
  ) {
    throw new Error("Invalid write acknowledgement status");
  }
  return {
    active: parseFilesystem(value.active),
    desired: parseFilesystem(value.desired),
    activation: value.activation,
    pending_restart: value.pending_restart,
    editable: value.editable,
  };
}

function gbToMiB(gb: number): number {
  return Math.round(gb * bytesPerGb) / bytesPerMiB;
}

function describeSettings(settings: FilesystemSettings): string {
  if (settings.write_ack_mode === "materialized") return "Materialized";
  const mib = gbToMiB(settings.volatile_memory_gb);
  return `Volatile memory · ${mib.toLocaleString()} MiB · ${settings.volatile_max_operations.toLocaleString()} operations`;
}

function canSendToken(): boolean {
  if (!window.isSecureContext) return false;
  const { protocol, hostname } = window.location;
  return (
    protocol === "https:" ||
    (protocol === "http:" &&
      (hostname === "localhost" ||
        hostname.endsWith(".localhost") ||
        /^127(?:\.\d{1,3}){3}$/.test(hostname) ||
        hostname === "[::1]"))
  );
}

const fieldClass = "mt-1 w-full rounded border border-border bg-background px-3 py-1.5 text-foreground";

function SettingsForm({ desired, onSaved }: { desired: FilesystemSettings; onSaved: (status: Status) => void }) {
  const [mode, setMode] = useState(desired.write_ack_mode);
  const [mib, setMiB] = useState(() =>
    desired.write_ack_mode === "volatile_memory" ? String(gbToMiB(desired.volatile_memory_gb)) : String(qualifiedMiB),
  );
  const [operations, setOperations] = useState(() =>
    desired.write_ack_mode === "volatile_memory"
      ? String(desired.volatile_max_operations)
      : String(qualifiedOperations),
  );
  const tokenInput = useRef<HTMLInputElement>(null);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const volatile = mode === "volatile_memory";

  async function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const input = tokenInput.current;
    if (saving || !input || !canSendToken()) return;
    const token = input.value.trim();
    input.value = "";
    const memory = Number(mib);
    const maxOperations = Number(operations);
    if (
      volatile &&
      (!Number.isInteger(memory) ||
        memory < 16 ||
        memory > qualifiedMiB ||
        !Number.isInteger(maxOperations) ||
        maxOperations < 1 ||
        maxOperations > qualifiedOperations)
    ) {
      setError("Enter whole numbers from 16 to 256 MiB and from 1 to 2,048 operations.");
      return;
    }
    if (!token) {
      setError("Enter an admin token to save.");
      return;
    }
    const filesystem: FilesystemSettings = volatile
      ? {
          write_ack_mode: "volatile_memory",
          volatile_memory_gb: (memory * bytesPerMiB) / bytesPerGb,
          volatile_max_operations: maxOperations,
        }
      : { write_ack_mode: "materialized" };
    setSaving(true);
    setError(null);
    try {
      const response = await fetch(endpoint, {
        method: "PUT",
        headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
        cache: "no-store",
        credentials: "omit",
        redirect: "error",
        body: JSON.stringify({ version: 1, filesystem }),
      });
      if (response.ok) {
        onSaved(await readStatus(response));
      } else if (response.status === 401 || response.status === 403) {
        setError("The server refused authorization. Check the admin token and try again.");
      } else if (response.status === 400 || response.status === 422) {
        setError("The server rejected these write acknowledgement settings.");
      } else {
        setError("Could not save write acknowledgement settings. Try again.");
      }
    } catch {
      setError("Could not confirm the save. Refresh the page to check the desired settings.");
    } finally {
      setSaving(false);
    }
  }

  return (
    <form onSubmit={save} autoComplete="off" noValidate className="border-t border-border pt-4">
      <fieldset disabled={saving} className="space-y-3">
        <div className="grid gap-3 md:grid-cols-3">
          <label className="block">
            Desired mode
            <select
              className={fieldClass}
              value={mode}
              onChange={(event) => setMode(event.target.value as FilesystemSettings["write_ack_mode"])}
            >
              <option value="materialized">Materialized</option>
              <option value="volatile_memory">Volatile memory</option>
            </select>
          </label>
          {volatile && (
            <>
              <label className="block">
                RAM limit (MiB)
                <input
                  className={fieldClass}
                  type="number"
                  min={16}
                  max={qualifiedMiB}
                  step={1}
                  value={mib}
                  onChange={(event) => setMiB(event.target.value)}
                />
              </label>
              <label className="block">
                Pending operation limit
                <input
                  className={fieldClass}
                  type="number"
                  min={1}
                  max={qualifiedOperations}
                  step={1}
                  value={operations}
                  onChange={(event) => setOperations(event.target.value)}
                />
              </label>
            </>
          )}
        </div>
        {volatile && (
          <p className="text-xs text-muted-foreground">Qualification used 256 MiB and 2,048 pending operations.</p>
        )}
        <div className="flex flex-wrap items-end gap-3">
          <label className="block min-w-0">
            Admin token
            <input className={fieldClass} type="password" ref={tokenInput} autoComplete="off" />
          </label>
          <Button type="submit" variant="primary">
            {saving ? "Saving…" : "Save for restart"}
          </Button>
        </div>
      </fieldset>
      {error && (
        <p role="alert" className="mt-3 text-destructive">
          {error}
        </p>
      )}
    </form>
  );
}

export function WriteAckSettings() {
  const [load, setLoad] = useState<Load>({ kind: "loading" });
  const [attempt, setAttempt] = useState(0);
  const [saved, setSaved] = useState(false);
  useEffect(() => {
    const controller = new AbortController();
    async function getStatus() {
      try {
        const response = await fetch(endpoint, {
          signal: controller.signal,
          cache: "no-store",
          credentials: "omit",
          redirect: "error",
        });
        // Older engines serve the application HTML for an unknown API path.
        const htmlFallback =
          response.ok && response.headers.get("content-type")?.split(";")[0]?.trim().toLowerCase() === "text/html";
        if (!controller.signal.aborted && (response.status === 404 || htmlFallback)) {
          setLoad({ kind: "unsupported" });
          return;
        }
        if (!response.ok) throw new Error("Write acknowledgement settings request failed");
        const status = await readStatus(response);
        if (!controller.signal.aborted) setLoad({ kind: "ready", status });
      } catch {
        if (!controller.signal.aborted) setLoad({ kind: "error" });
      }
    }
    void getStatus();
    return () => controller.abort();
  }, [attempt]);

  if (load.kind === "unsupported") return null;
  const secure = canSendToken();
  return (
    <section aria-labelledby="write-ack-title" className="card-surface space-y-4 rounded-lg p-5 text-sm">
      <h2 id="write-ack-title" className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
        Write acknowledgement
      </h2>
      {load.kind === "loading" && <p role="status">Loading write acknowledgement settings…</p>}
      {load.kind === "error" && (
        <div className="flex flex-wrap items-center gap-3">
          <p role="alert">Could not load write acknowledgement settings.</p>
          <Button
            type="button"
            variant="outline"
            onClick={() => {
              setLoad({ kind: "loading" });
              setAttempt((attempt) => attempt + 1);
            }}
          >
            Retry
          </Button>
        </div>
      )}
      {load.kind === "ready" && (
        <>
          <dl className="grid gap-3 sm:grid-cols-2">
            <div>
              <dt className="text-muted">Active</dt>
              <dd>{describeSettings(load.status.active)}</dd>
            </div>
            <div>
              <dt className="text-muted">Desired</dt>
              <dd>{describeSettings(load.status.desired)}</dd>
            </div>
          </dl>
          <p role="status" className={load.status.pending_restart ? "text-warning" : "text-muted"}>
            {saved && "Settings saved. "}
            {load.status.pending_restart
              ? "Restart required to activate the desired settings."
              : "Changes take effect after restart."}
          </p>
          <p className="text-muted-foreground">
            Volatile memory can acknowledge an ordinary WRITE while its data is still in RAM. A crash can lose those
            writes. Explicit fsync and protocol durability requests still wait for their configured durability boundary.
          </p>
          {load.status.editable && secure ? (
            <SettingsForm
              key={JSON.stringify(load.status.desired)}
              desired={load.status.desired}
              onSaved={(status) => {
                setLoad({ kind: "ready", status });
                setSaved(true);
              }}
            />
          ) : (
            <p className="text-muted-foreground">
              {load.status.editable
                ? "Open this page over HTTPS or on localhost to edit these settings."
                : "The server has made these settings read-only."}
            </p>
          )}
        </>
      )}
    </section>
  );
}
