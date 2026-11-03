type ReadAt = (offset: bigint, length: number) => Promise<Uint8Array>;
type ReadResult =
  | { ok: true; data: Uint8Array; eof: boolean }
  | { ok: false; error: unknown };

const READ_DEPTH = 8;
const MAX_CHUNK = 1024 * 1024;

/** Read an open file in order, with at most eight 1 MiB ranges in flight.
 * Short nonempty replies are refilled; only an empty reply establishes EOF.
 * All issued reads settle before returning, so the caller may close its handle.
 */
export async function readFileChunks(
  readAt: ReadAt,
  maxReadChunk: number,
  onProgress?: (received: number) => void,
  signal?: AbortSignal,
): Promise<Uint8Array[]> {
  if (!Number.isSafeInteger(maxReadChunk) || maxReadChunk <= 0) {
    throw new Error("Read chunk size must be a positive integer");
  }
  const chunkSize = Math.min(maxReadChunk, MAX_CHUNK);
  const pending: Promise<ReadResult>[] = [];
  const chunks: Uint8Array[] = [];
  let nextOffset = 0n;
  let received = 0;
  let stopped = false;
  const aborted = () => new DOMException("Aborted", "AbortError");

  const readRange = async (offset: bigint): Promise<ReadResult> => {
    try {
      let filled = 0;
      let buffer: Uint8Array | undefined;
      while (filled < chunkSize) {
        if (stopped || signal?.aborted) throw aborted();
        const data = await readAt(offset + BigInt(filled), chunkSize - filled);
        if (data.length > chunkSize - filled) {
          throw new Error("Read reply exceeds the requested range");
        }
        if (data.length === 0) {
          return { ok: true, data: buffer?.subarray(0, filled) ?? data, eof: true };
        }
        if (filled === 0 && data.length === chunkSize) {
          return { ok: true, data, eof: false };
        }
        buffer ??= new Uint8Array(chunkSize);
        buffer.set(data, filled);
        filled += data.length;
      }
      return { ok: true, data: buffer!, eof: false };
    } catch (error) {
      // Handle speculative rejection immediately, even while an earlier range
      // is still awaited. The ordered consumer propagates the relevant error.
      return { ok: false, error };
    }
  };

  try {
    for (;;) {
      if (signal?.aborted) throw aborted();
      while (pending.length < READ_DEPTH) {
        pending.push(readRange(nextOffset));
        nextOffset += BigInt(chunkSize);
      }
      const result = await pending.shift()!;
      if (signal?.aborted) throw aborted();
      if (!result.ok) throw result.error;
      if (result.data.length > 0) {
        chunks.push(result.data);
        received += result.data.length;
        onProgress?.(received);
      }
      if (result.eof) break;
    }
  } finally {
    stopped = true;
    await Promise.all(pending);
  }
  if (signal?.aborted) throw aborted();
  return chunks;
}
