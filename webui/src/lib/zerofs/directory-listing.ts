interface WireName {
  name: string;
  name_bytes: ArrayLike<number>;
  name_is_utf8: boolean;
}

export interface DirectoryListing<T> {
  entries: T[];
  rawNames: string[];
}

export function directoryListing<T extends WireName, U>(wire: T[], fileEntry: (entry: T) => U): DirectoryListing<U> {
  const entries: U[] = [];
  const rawNames: string[] = [];
  for (const entry of wire) {
    if (entry.name_is_utf8 === true) entries.push(fileEntry(entry));
    else rawNames.push(Array.from(entry.name_bytes, (byte) => byte.toString(16).padStart(2, "0")).join(""));
  }
  return { entries, rawNames };
}

export class RawFilenameError extends Error {}

export function textPathEntries<T>(directory: DirectoryListing<T>, path: string): T[] {
  if (directory.rawNames.length) {
    throw new RawFilenameError(`Cannot traverse ${path}: filename bytes ${directory.rawNames[0]} cannot be addressed by this browser.`);
  }
  return directory.entries;
}
