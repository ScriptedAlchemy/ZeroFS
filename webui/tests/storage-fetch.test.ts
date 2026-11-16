import assert from "node:assert/strict";
import test from "node:test";
import { Code, ConnectError } from "@connectrpc/connect";
import { createStorageFetch, MAX_STORAGE_MESSAGE_BYTES } from "../src/lib/grpc/storage-fetch.ts";

const methods = ["GetPoolStatus", "GetPoolAffected"];
const url = (method: string) => `http://localhost/zerofs.admin.AdminService/${method}`;
const headers = { "content-type": "application/grpc-web+proto" };

function frameHeader(length: number, flags = 0): Uint8Array {
  const header = new Uint8Array(5);
  header[0] = flags;
  new DataView(header.buffer).setUint32(1, length);
  return header;
}

test("oversized storage frames are rejected from a fragmented header and cancel the download", async () => {
  for (const method of methods) {
    let cancelReason: unknown;
    const header = frameHeader(MAX_STORAGE_MESSAGE_BYTES + 1);
    const response = new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(header.subarray(0, 4));
          controller.enqueue(header.subarray(4));
          // No body is sent: the frame must fail before buffering any payload.
        },
        cancel(reason) {
          cancelReason = reason;
        },
      }),
      { headers },
    );
    const bounded = await createStorageFetch(async () => response)(url(method));
    const reader = bounded.body!.getReader();
    assert.deepEqual((await reader.read()).value, header.subarray(0, 4));
    await assert.rejects(reader.read(), (error: unknown) => {
      assert.ok(error instanceof ConnectError);
      assert.equal(error.code, Code.ResourceExhausted);
      assert.match(error.message, /too large to display safely/);
      return true;
    });
    await new Promise((resolve) => setImmediate(resolve));
    assert.ok(cancelReason instanceof ConnectError);
  }
});

test("bounded responses preserve protobuf payloads and trailers across every chunk boundary", async () => {
  const payload = Uint8Array.from([10, 4, 123, 125, 0, 255]);
  const trailer = new TextEncoder().encode("grpc-status: 0\r\n");
  const wire = Uint8Array.from([
    ...frameHeader(payload.length),
    ...payload,
    ...frameHeader(trailer.length, 128),
    ...trailer,
  ]);
  for (let split = 1; split < wire.length; split += 1) {
    const response = new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(wire.subarray(0, split));
          controller.enqueue(wire.subarray(split));
          controller.close();
        },
      }),
      { status: 200, headers },
    );
    const bounded = await createStorageFetch(async () => response)(new Request(url("GetPoolStatus")));
    assert.deepEqual(new Uint8Array(await bounded.arrayBuffer()), wire);
    assert.equal(bounded.headers.get("content-type"), headers["content-type"]);
  }
});

test("truncated storage frames fail closed", async () => {
  for (const wire of [frameHeader(1).subarray(0, 4), frameHeader(1)]) {
    const response = new Response(Uint8Array.from(wire), { headers });
    const bounded = await createStorageFetch(async () => response)(url("GetPoolAffected"));
    await assert.rejects(bounded.arrayBuffer(), (error: unknown) => {
      assert.ok(error instanceof ConnectError);
      assert.equal(error.code, Code.DataLoss);
      return true;
    });
  }
});

test("consumer cancellation closes the bounded source stream", async () => {
  let cancelled = false;
  const response = new Response(
    new ReadableStream<Uint8Array>({
      cancel() {
        cancelled = true;
      },
    }),
    { headers },
  );
  const bounded = await createStorageFetch(async () => response)(url("GetPoolAffected"));
  await bounded.body!.cancel();
  assert.equal(cancelled, true);
});

test("unrelated streams and HTTP errors retain their original response", async () => {
  for (const [method, response] of [
    ["StreamStats", new Response("unbounded stream", { headers })],
    ["GetPoolAffected", new Response("service down", { status: 503, headers })],
    ["GetPoolStatus", new Response("not grpc", { headers: { "content-type": "text/html" } })],
  ] as const) {
    const abort = new AbortController();
    const request = url(method);
    const init = { signal: abort.signal };
    const bounded = await createStorageFetch(async (input, options) => {
      assert.equal(input, request);
      assert.equal(options, init);
      return response;
    })(request, init);
    assert.equal(bounded, response);
  }
});
