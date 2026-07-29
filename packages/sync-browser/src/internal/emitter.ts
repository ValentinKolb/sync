export class Emitter<T = void> {
  private listeners = new Set<(value: T) => void>();

  /** Subscribe to events. Returns an unsubscribe function. */
  on(fn: (value: T) => void): () => void {
    this.listeners.add(fn);
    return () => {
      this.listeners.delete(fn);
    };
  }

  /** Emit a value to all current listeners. */
  emit(value: T): void {
    for (const fn of this.listeners) {
      fn(value);
    }
  }

  /** Returns a promise that resolves on the next emit. */
  once(): Promise<T> {
    return new Promise<T>((resolve) => {
      const unsub = this.on((value) => {
        unsub();
        resolve(value);
      });
    });
  }

  /**
   * Wait for the next emit, a timeout, or an abort — whichever comes first, and
   * always unsubscribing.
   *
   * `once()` has no cancellation path: its listener is removed only when it
   * fires. Racing it against a timer therefore leaked one permanently
   * registered closure per lost race — an idle job worker polls once a second
   * with no signal, so it accumulated about 3600 listeners per idle hour, each
   * retaining a promise and its resolve, and every emit() then iterated all of
   * them. Nothing could remove them, not even stop(), because they live on the
   * shared state rather than on the worker.
   *
   * @returns true if an emit arrived, false on timeout or abort.
   */
  waitFor(timeoutMs: number, signal?: AbortSignal): Promise<boolean> {
    if (signal?.aborted) return Promise.resolve(false);

    return new Promise<boolean>((resolve) => {
      let settled = false;
      const finish = (emitted: boolean): void => {
        if (settled) return;
        settled = true;
        unsub();
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        resolve(emitted);
      };

      const unsub = this.on(() => finish(true));
      const timer = setTimeout(() => finish(false), timeoutMs);
      const onAbort = (): void => finish(false);
      signal?.addEventListener("abort", onAbort, { once: true });
    });
  }

  /** Returns a promise that resolves on the next emit, or rejects on abort. */
  onceWithSignal(signal?: AbortSignal): Promise<T> {
    if (!signal) return this.once();
    if (signal.aborted) return Promise.reject(Object.assign(new Error("aborted"), { name: "AbortError" }));

    return new Promise<T>((resolve, reject) => {
      const unsub = this.on((value) => {
        unsub();
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      });

      const onAbort = (): void => {
        unsub();
        signal.removeEventListener("abort", onAbort);
        reject(Object.assign(new Error("aborted"), { name: "AbortError" }));
      };

      signal.addEventListener("abort", onAbort, { once: true });
    });
  }
}
