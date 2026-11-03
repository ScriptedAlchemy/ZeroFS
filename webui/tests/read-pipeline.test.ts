import assert from "node:assert/strict";
import test from "node:test";
import { readFileChunks } from "../src/lib/zerofs/read-pipeline.ts";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((ok, fail) => {
    resolve = ok;
    reject = fail;
  });
  return { promise, resolve, reject };
}

function joined(chunks: Uint8Array[]) {
  return Buffer.concat(chunks);
}

test("fills eight slots before any reply and preserves byte order", async () => {
  const size = 16;
  const payload = Uint8Array.from({ length: size * 9 + 3 }, (_, i) => i % 251);
  const firstWave: { offset: number; length: number; reply: ReturnType<typeof deferred<Uint8Array>> }[] = [];
  const progress: number[] = [];
  let active = 0;
  let peak = 0;
  let calls = 0;
  const result = readFileChunks(async (offset, length) => {
    active++;
    peak = Math.max(peak, active);
    try {
      if (calls++ < 8) {
        const reply = deferred<Uint8Array>();
        firstWave.push({ offset: Number(offset), length, reply });
        return await reply.promise;
      }
      return payload.slice(Number(offset), Number(offset) + length);
    } finally {
      active--;
    }
  }, size, (received) => progress.push(received));
  assert.equal(firstWave.length, 8);
  for (const { offset, length, reply } of firstWave.toReversed()) {
    reply.resolve(payload.slice(offset, offset + length));
  }
  assert.deepEqual(joined(await result), Buffer.from(payload));
  assert.equal(peak, 8);
  assert.equal(active, 0);
  assert.equal(progress.at(-1), payload.length);
  assert.deepEqual(progress, progress.toSorted((a, b) => a - b));
});

test("refills short nonempty replies without skipping data", async () => {
  const payload = Uint8Array.from({ length: 103 }, (_, i) => i);
  const chunks = await readFileChunks(async (offset, length) => {
    const take = Math.max(1, Math.floor(length / 2));
    return payload.slice(Number(offset), Number(offset) + take);
  }, 16);
  assert.deepEqual(joined(chunks), Buffer.from(payload));
});

test("an earlier EOF discards later speculative errors", async () => {
  const chunks = await readFileChunks(async (offset) => {
    if (offset === 0n) return Uint8Array.of(10, 20);
    if (offset === 2n) return new Uint8Array();
    throw new Error("speculative range after EOF");
  }, 16);
  assert.deepEqual(joined(chunks), Buffer.from([10, 20]));
});

test("abort on the final progress callback prevents a completed download", async () => {
  const controller = new AbortController();
  await assert.rejects(readFileChunks(async (offset) => {
    return offset === 0n ? Uint8Array.of(1, 2) : new Uint8Array();
  }, 16, () => controller.abort(), controller.signal), { name: "AbortError" });
});

test("abort during EOF cleanup is checked after pending reads settle", async () => {
  const controller = new AbortController();
  const pending: ReturnType<typeof deferred<Uint8Array>>[] = [];
  const result = readFileChunks(async (offset) => {
    if (offset === 0n) return new Uint8Array();
    const reply = deferred<Uint8Array>();
    pending.push(reply);
    return reply.promise;
  }, 16, undefined, controller.signal);
  const checked = assert.rejects(result, { name: "AbortError" });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(pending.length, 7);
  controller.abort();
  for (const reply of pending) reply.resolve(new Uint8Array());
  await checked;
});

for (const failure of ["abort", "read error"] as const) {
  test(`${failure} settles every issued read before returning`, async () => {
    const controller = new AbortController();
    const pending: ReturnType<typeof deferred<Uint8Array>>[] = [];
    let returned = false;
    const result = readFileChunks(() => {
      const reply = deferred<Uint8Array>();
      pending.push(reply);
      return reply.promise;
    }, 16, undefined, controller.signal);
    const checked = assert.rejects(result, failure === "abort" ? { name: "AbortError" } : /failed read/);
    void result.then(() => { returned = true; }, () => { returned = true; });
    assert.equal(pending.length, 8);
    if (failure === "abort") {
      controller.abort();
      pending[0].resolve(new Uint8Array(16));
    } else {
      pending[0].reject(new Error("failed read"));
    }
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(returned, false, "caller must not close a handle with reads pending");
    for (const reply of pending.slice(1)) reply.resolve(new Uint8Array());
    await checked;
    assert.equal(returned, true);
    assert.equal(pending.length, 8, "abort/error must stop refilling the pipeline");
  });
}

test("caps requests and rejects invalid chunk sizes before issuing reads", async () => {
  const lengths: number[] = [];
  await readFileChunks(async (_offset, length) => {
    lengths.push(length);
    return new Uint8Array();
  }, 10 * 1024 * 1024);
  assert.equal(lengths.length, 8);
  assert(lengths.every((length) => length === 1024 * 1024));
  for (const invalid of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
    await assert.rejects(readFileChunks(async () => {
      assert.fail("invalid size issued a read");
    }, invalid), /positive integer/);
  }
});
