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
