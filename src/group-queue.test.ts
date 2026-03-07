import { describe, it, expect } from 'vitest';

import { GroupQueue } from './group-queue.js';

describe('GroupQueue', () => {
  it('serializes tasks within the same group', async () => {
    const queue = new GroupQueue();
    const order: number[] = [];

    await Promise.all([
      queue.enqueue('group-a', async () => {
        await new Promise(r => setTimeout(r, 50));
        order.push(1);
      }),
      queue.enqueue('group-a', async () => {
        order.push(2);
      }),
    ]);

    expect(order).toEqual([1, 2]);
  });

  it('runs different groups in parallel', async () => {
    const queue = new GroupQueue();
    const running: string[] = [];
    const snapshots: string[][] = [];

    const task = (group: string) => async () => {
      running.push(group);
      snapshots.push([...running]);
      await new Promise(r => setTimeout(r, 50));
      running.splice(running.indexOf(group), 1);
    };

    await Promise.all([
      queue.enqueue('group-a', task('a')),
      queue.enqueue('group-b', task('b')),
    ]);

    // Both should have been running at the same time at some point
    const concurrent = snapshots.some(s => s.length === 2);
    expect(concurrent).toBe(true);
  });

  it('continues the chain even if a task throws', async () => {
    const queue = new GroupQueue();
    const results: string[] = [];

    await queue.enqueue('group-a', async () => {
      throw new Error('boom');
    }).catch(() => {});

    await queue.enqueue('group-a', async () => {
      results.push('recovered');
    });

    expect(results).toEqual(['recovered']);
  });

  it('handles empty queue gracefully', async () => {
    const queue = new GroupQueue();
    // Enqueue and immediately await — should resolve cleanly
    await queue.enqueue('group-a', async () => {});
  });
});
