import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { _initTestDatabase, createTask, getDueTasks, claimTask, getTaskById } from './db.js';
import {
  _resetSchedulerLoopForTests,
  computeNextRun,
  startSchedulerLoop,
} from './task-scheduler.js';
import { GroupQueue } from './group-queue.js';

describe('task scheduler', () => {
  beforeEach(() => {
    _initTestDatabase();
    _resetSchedulerLoopForTests();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe('computeNextRun', () => {
    it('returns null for once-tasks', () => {
      expect(computeNextRun({
        id: 'once-test',
        schedule_type: 'once',
        schedule_value: '2026-01-01T00:00:00.000Z',
        next_run: new Date(Date.now() - 1000).toISOString(),
      })).toBeNull();
    });

    it('anchors interval tasks to scheduled time to prevent drift', () => {
      const scheduledTime = new Date(Date.now() - 2000).toISOString(); // 2s ago

      const nextRun = computeNextRun({
        id: 'drift-test',
        schedule_type: 'interval',
        schedule_value: '60000', // 1 minute
        next_run: scheduledTime,
      });

      expect(nextRun).not.toBeNull();
      // Should be anchored to scheduledTime + 60s, NOT Date.now() + 60s
      const expected = new Date(scheduledTime).getTime() + 60000;
      expect(new Date(nextRun!).getTime()).toBe(expected);
    });

    it('skips missed intervals without infinite loop', () => {
      const ms = 60000;
      const missedBy = ms * 10;
      const scheduledTime = new Date(Date.now() - missedBy).toISOString();

      const nextRun = computeNextRun({
        id: 'skip-test',
        schedule_type: 'interval',
        schedule_value: String(ms),
        next_run: scheduledTime,
      });

      expect(nextRun).not.toBeNull();
      // Must be in the future
      expect(new Date(nextRun!).getTime()).toBeGreaterThan(Date.now());
      // Must be aligned to the original schedule grid
      const offset = (new Date(nextRun!).getTime() - new Date(scheduledTime).getTime()) % ms;
      expect(offset).toBe(0);
    });
  });

  describe('claim-before-enqueue', () => {
    it('claimTask with future next_run prevents getDueTasks from returning the task', () => {
      const pastTime = new Date(Date.now() - 60_000).toISOString();
      const futureTime = new Date(Date.now() + 3_600_000).toISOString();

      createTask({
        id: 'task-1', group_folder: 'test-group', chat_jid: 'chat-1',
        prompt: 'test', schedule_type: 'cron', schedule_value: '0 * * * *',
        next_run: pastTime, status: 'active', created_at: pastTime,
      });

      expect(getDueTasks()).toHaveLength(1);

      claimTask('task-1', futureTime);

      expect(getDueTasks()).toHaveLength(0);
      expect(getTaskById('task-1')?.next_run).toBe(futureTime);
    });

    it('claimTask with null (once task) prevents re-pickup', () => {
      const pastTime = new Date(Date.now() - 60_000).toISOString();

      createTask({
        id: 'task-once', group_folder: 'test-group', chat_jid: 'chat-1',
        prompt: 'run once', schedule_type: 'once', schedule_value: '',
        next_run: pastTime, status: 'active', created_at: pastTime,
      });

      expect(getDueTasks()).toHaveLength(1);
      claimTask('task-once', null);
      expect(getDueTasks()).toHaveLength(0);
    });

    it('scheduler loop claims all due tasks synchronously before any run', async () => {
      const pastTime = new Date(Date.now() - 60_000).toISOString();

      for (let i = 1; i <= 3; i++) {
        createTask({
          id: `task-${i}`, group_folder: 'test-group', chat_jid: 'chat-1',
          prompt: `task ${i}`, schedule_type: 'interval', schedule_value: '3600000',
          next_run: pastTime, status: 'active', created_at: pastTime,
        });
      }

      expect(getDueTasks()).toHaveLength(3);

      const enqueuedFns: Array<() => Promise<void>> = [];
      const queue = new GroupQueue();
      const originalEnqueue = queue.enqueue.bind(queue);
      vi.spyOn(queue, 'enqueue').mockImplementation((_group, fn) => {
        enqueuedFns.push(fn);
        return Promise.resolve();
      });

      startSchedulerLoop({
        sendMessage: async () => {},
        registeredGroups: () => ({}),
        buildContextPrefix: () => '',
        queue,
      });

      await vi.advanceTimersByTimeAsync(10);

      // All 3 tasks should have been enqueued
      expect(enqueuedFns).toHaveLength(3);
      // And none should be due anymore (claimed before any fn ran)
      expect(getDueTasks()).toHaveLength(0);
    });
  });
});
