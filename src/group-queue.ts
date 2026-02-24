/**
 * Per-group async queue to serialize container execution.
 * Prevents concurrent containers for the same group while
 * allowing different groups to run in parallel.
 */
export class GroupQueue {
  private chains = new Map<string, Promise<void>>();

  async enqueue(groupFolder: string, fn: () => Promise<void>): Promise<void> {
    const prev = this.chains.get(groupFolder) ?? Promise.resolve();
    const next = prev.then(fn, fn); // run fn even if previous failed
    this.chains.set(groupFolder, next);
    return next;
  }
}
