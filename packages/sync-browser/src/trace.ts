export type TraceHandler<Event> = (event: Event) => void | Promise<void>;

export const emitTrace = async <Event>(trace: TraceHandler<Event> | undefined, event: Event): Promise<void> => {
  if (!trace) return;
  try {
    await trace(event);
  } catch (error) {
    console.warn("[sync trace] trace handler failed", error);
  }
};
