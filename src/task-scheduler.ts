import fs from 'fs';
import pino from 'pino';
import { CronExpressionParser } from 'cron-parser';
import { getDueTasks, claimTask, updateTaskAfterRun, logTaskRun, getTaskById, getAllTasks } from './db.js';
import { ScheduledTask, RegisteredGroup } from './types.js';
import { SCHEDULER_POLL_INTERVAL, MAIN_GROUP_FOLDER, TIMEZONE } from './config.js';
import { runContainerAgent, writeTasksSnapshot } from './container-runner.js';
import { resolveGroupFolderPath } from './group-folder.js';
import { GroupQueue } from './group-queue.js';

const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  transport: { target: 'pino-pretty', options: { colorize: true } }
});

/**
 * Compute the next run time for a recurring task, anchored to the
 * task's scheduled time rather than Date.now() to prevent cumulative drift.
 */
export function computeNextRun(task: Pick<ScheduledTask, 'id' | 'schedule_type' | 'schedule_value' | 'next_run'>): string | null {
  if (task.schedule_type === 'once') return null;

  const now = Date.now();

  if (task.schedule_type === 'cron') {
    const interval = CronExpressionParser.parse(task.schedule_value, { tz: TIMEZONE });
    return interval.next().toISOString();
  }

  if (task.schedule_type === 'interval') {
    const ms = parseInt(task.schedule_value, 10);
    if (!ms || ms <= 0) {
      logger.warn({ taskId: task.id, value: task.schedule_value }, 'Invalid interval value');
      return new Date(now + 60_000).toISOString();
    }
    // Anchor to the scheduled time, not now, to prevent drift.
    // Skip past any missed intervals so we always land in the future.
    let next = new Date(task.next_run!).getTime() + ms;
    while (next <= now) {
      next += ms;
    }
    return new Date(next).toISOString();
  }

  return null;
}

export interface SchedulerDependencies {
  sendMessage: (jid: string, text: string) => Promise<void>;
  registeredGroups: () => Record<string, RegisteredGroup>;
  buildContextPrefix: (chatJid: string, groupFolder: string) => string;
  queue: GroupQueue;
}

async function runTask(task: ScheduledTask, nextRun: string | null, deps: SchedulerDependencies): Promise<void> {
  const startTime = Date.now();

  const finalize = (error: string | null, result: string | null = null) => {
    logTaskRun({
      task_id: task.id,
      run_at: new Date().toISOString(),
      duration_ms: Date.now() - startTime,
      status: error ? 'error' : 'success',
      result,
      error
    });
    const resultSummary = error ? `Error: ${error}` : (result ? result.slice(0, 200) : 'Completed');
    updateTaskAfterRun(task.id, nextRun, resultSummary);
  };

  let groupDir: string;
  try {
    groupDir = resolveGroupFolderPath(task.group_folder);
  } catch (err) {
    const error = err instanceof Error ? err.message : String(err);
    logger.error({ taskId: task.id, groupFolder: task.group_folder, error }, 'Task has invalid group folder');
    finalize(error);
    return;
  }
  fs.mkdirSync(groupDir, { recursive: true });

  logger.info({ taskId: task.id, group: task.group_folder }, 'Running scheduled task');

  const groups = deps.registeredGroups();
  const group = Object.values(groups).find(g => g.folder === task.group_folder);

  if (!group) {
    const error = `Group not found: ${task.group_folder}`;
    logger.error({ taskId: task.id, groupFolder: task.group_folder }, 'Group not found for task');
    finalize(error);
    return;
  }

  // Update tasks snapshot for container to read (filtered by group)
  const isMain = task.group_folder === MAIN_GROUP_FOLDER;
  const tasks = getAllTasks();
  writeTasksSnapshot(task.group_folder, isMain, tasks.map(t => ({
    id: t.id,
    groupFolder: t.group_folder,
    prompt: t.prompt,
    schedule_type: t.schedule_type,
    schedule_value: t.schedule_value,
    status: t.status,
    next_run: t.next_run
  })));

  let result: string | null = null;
  let error: string | null = null;

  // Prepend context history for fresh session awareness
  const contextPrefix = deps.buildContextPrefix(task.chat_jid, task.group_folder);
  const fullPrompt = contextPrefix + task.prompt;

  try {
    const output = await runContainerAgent(group, {
      prompt: fullPrompt,
      groupFolder: task.group_folder,
      chatJid: task.chat_jid,
      isMain,
      isScheduledTask: true,
      triggerType: 'scheduled_task',
      taskId: task.id
    });

    if (output.status === 'error') {
      error = output.error || 'Unknown error';
    } else {
      result = output.result;
    }

    logger.info({ taskId: task.id, durationMs: Date.now() - startTime }, 'Task completed');
  } catch (err) {
    error = err instanceof Error ? err.message : String(err);
    logger.error({ taskId: task.id, error }, 'Task failed');
  }

  finalize(error, result);
}

let schedulerRunning = false;

export function startSchedulerLoop(deps: SchedulerDependencies): void {
  if (schedulerRunning) {
    logger.debug('Scheduler loop already running, skipping duplicate start');
    return;
  }
  schedulerRunning = true;
  logger.info('Scheduler loop started');

  const loop = async () => {
    try {
      const dueTasks = getDueTasks();
      if (dueTasks.length > 0) {
        logger.info({ count: dueTasks.length }, 'Found due tasks');
      }

      for (const task of dueTasks) {
        // Re-check task status in case it was paused/cancelled
        const currentTask = getTaskById(task.id);
        if (!currentTask || currentTask.status !== 'active') {
          continue;
        }

        // Claim task BEFORE enqueuing to prevent the next poll from
        // picking it up again while it waits in the queue
        const nextRun = computeNextRun(currentTask);
        claimTask(currentTask.id, nextRun);

        // Enqueue per group — serializes with message processing
        deps.queue.enqueue(currentTask.group_folder, () => runTask(currentTask, nextRun, deps))
          .catch(err => logger.error({ taskId: currentTask.id, err }, 'Unhandled error in scheduled task'));
      }
    } catch (err) {
      logger.error({ err }, 'Error in scheduler loop');
    }

    setTimeout(loop, SCHEDULER_POLL_INTERVAL);
  };

  loop();
}

/** @internal — for tests only. */
export function _resetSchedulerLoopForTests(): void {
  schedulerRunning = false;
}
