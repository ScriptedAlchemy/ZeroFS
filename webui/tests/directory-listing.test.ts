import assert from "node:assert/strict";
import test from "node:test";
import { pooled } from "../src/lib/async.ts";
import { directoryListing, RawFilenameError, textPathEntries } from "../src/lib/zerofs/directory-listing.ts";

test("bulk selection exposes the literal replacement-character name and preserves the distinct raw filename", () => {
  const listing = directoryListing([
    { name: "�", name_bytes: [0xff], name_is_utf8: false, ino: 1 },
    { name: "�", name_bytes: [0xef, 0xbf, 0xbd], name_is_utf8: true, ino: 2 },
    { name: "�", name_bytes: [0xfe], name_is_utf8: false, ino: 3 },
  ], ({ name, ino }) => ({ name, ino }));
  assert.deepEqual(listing, { entries: [{ name: "�", ino: 2 }], rawNames: ["ff", "fe"] });
  assert.throws(() => textPathEntries(listing, "/music"), (error) =>
    error instanceof RawFilenameError && /\/music.*filename bytes ff/.test(error.message));
});

test("a valid replacement character and a literal hex-looking name remain addressable", () => {
  const listing = directoryListing([
    { name: "�", name_bytes: [0xef, 0xbf, 0xbd], name_is_utf8: true },
    { name: "ff", name_bytes: [0x66, 0x66], name_is_utf8: true },
  ], ({ name }) => name);
  assert.deepEqual(textPathEntries(listing, "/"), ["�", "ff"]);
});

test("a rejected traversal drains active siblings before reporting failure and schedules no further siblings", async () => {
  let finish!: () => void;
  const active = new Promise<void>((resolve) => { finish = resolve; });
  const started: number[] = [];
  let settled = false;
  const result = pooled([1, 2, 3], 2, async (n) => {
    started.push(n);
    if (n === 1) throw new RawFilenameError("raw filename");
    await active;
  }).finally(() => { settled = true; });
  await new Promise<void>((resolve) => setTimeout(resolve, 0));
  assert.deepEqual(started, [1, 2]);
  assert.equal(settled, false);
  finish();
  await assert.rejects(result, RawFilenameError);
  assert.deepEqual(started, [1, 2]);
});
