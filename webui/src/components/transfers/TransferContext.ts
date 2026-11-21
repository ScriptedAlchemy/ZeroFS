import { createContext } from "react";

export interface TransferHandle {
  id: number;
  signal: AbortSignal;
  setName: (name: string) => void;
  update: (received: number, total: number, detail?: string) => void;
  finish: () => void;
  fail: (error: string) => void;
}

// False keeps the failed transfer visible when retry needs a user decision or
// cannot start from the current page. Existing fire-and-forget retries return void.
export type TransferRetry = () => void | boolean | Promise<void | boolean>;

export interface TransferContextValue {
  startDownload: (name: string, retry?: TransferRetry) => TransferHandle;
  startUpload: (name: string, total: number, retry?: TransferRetry) => TransferHandle;
  startDelete: (name: string, retry?: TransferRetry) => TransferHandle;
}

const noop: TransferHandle = {
  id: 0,
  signal: new AbortController().signal,
  setName() {},
  update() {},
  finish() {},
  fail() {},
};

export const TransferContext = createContext<TransferContextValue>({
  startDownload: () => noop,
  startUpload: () => noop,
  startDelete: () => noop,
});
