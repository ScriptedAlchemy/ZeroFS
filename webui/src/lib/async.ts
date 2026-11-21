/** Run async tasks with bounded concurrency and drain active tasks before returning a failure. */
export async function pooled<T>(items: T[], concurrency: number, fn: (item: T) => Promise<void>): Promise<void> {
  let i = 0;
  let failure: { error: unknown } | undefined;
  const next = async (): Promise<void> => {
    while (!failure && i < items.length) {
      const item = items[i++];
      try {
        await fn(item);
      } catch (error) {
        failure ??= { error };
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, () => next()));
  if (failure) throw failure.error;
}
