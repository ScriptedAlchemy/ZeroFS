import { Code, ConnectError } from "@connectrpc/connect";

// Pool observations embed JSON in one protobuf string. Bound the wire frame before
// connect-web sees its header. Its decoder allocates the declared message length
// immediately, so checking response.json.length in the page is too late.
export const MAX_STORAGE_MESSAGE_BYTES = 8 * 1024 * 1024 + 1024;
const MAX_STORAGE_RESPONSE_BYTES = MAX_STORAGE_MESSAGE_BYTES + 16 * 1024;
const storageMethods = new Set([
  "/zerofs.admin.AdminService/GetPoolStatus",
  "/zerofs.admin.AdminService/GetPoolAffected",
]);

function tooLarge(): ConnectError {
  return new ConnectError(
    "The storage response is too large to display safely (8 MiB limit). The admin API needs paging for this result.",
    Code.ResourceExhausted,
  );
}

/** Limit only the unary pool JSON replies; long-lived stats streams remain streaming. */
export function createStorageFetch(fetchImpl: typeof fetch): typeof fetch {
  return async (input, init) => {
    const response = await fetchImpl(input, init);
    const url = new URL(input instanceof Request ? input.url : String(input), "http://localhost");
    if (
      !storageMethods.has(url.pathname) ||
      !response.ok ||
      !response.body ||
      !/^application\/grpc-web(?:\+proto)?(?:\s*;|$)/i.test(response.headers.get("content-type") ?? "")
    )
      return response;

    const header = new Uint8Array(5);
    const headerView = new DataView(header.buffer);
    let headerBytes = 0;
    let bodyBytes = 0;
    let receivedBytes = 0;
    const bounded = response.body.pipeThrough(
      new TransformStream<Uint8Array, Uint8Array>({
        transform(chunk, controller) {
          receivedBytes += chunk.byteLength;
          if (receivedBytes > MAX_STORAGE_RESPONSE_BYTES) throw tooLarge();
          let offset = 0;
          while (offset < chunk.byteLength) {
            if (bodyBytes > 0) {
              const length = Math.min(bodyBytes, chunk.byteLength - offset);
              bodyBytes -= length;
              offset += length;
            } else {
              const length = Math.min(header.length - headerBytes, chunk.byteLength - offset);
              header.set(chunk.subarray(offset, offset + length), headerBytes);
              headerBytes += length;
              offset += length;
              if (headerBytes === header.length) {
                bodyBytes = headerView.getUint32(1);
                if (bodyBytes > MAX_STORAGE_MESSAGE_BYTES) throw tooLarge();
                headerBytes = 0;
              }
            }
          }
          // Never forward the chunk completing an oversized frame header. A partial
          // header can be forwarded safely; Connect cannot allocate from it yet.
          controller.enqueue(chunk);
        },
        flush() {
          if (headerBytes !== 0 || bodyBytes !== 0)
            throw new ConnectError("Incomplete storage response", Code.DataLoss);
        },
      }),
    );
    return new Response(bounded, {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers,
    });
  };
}
