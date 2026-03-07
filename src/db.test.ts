import { describe, it, expect, beforeEach } from 'vitest';

import {
  _initTestDatabase,
  createTask,
  deleteTask,
  getAllChats,
  getDueTasks,
  claimTask,
  getTaskById,
  getTasksForGroup,
  getAllTasks,
  updateTask,
  updateTaskAfterRun,
  logTaskRun,
  getTaskRunLogs,
  storeChatMetadata,
  storeGenericMessage,
  getMessagesSince,
  getNewMessages,
} from './db.js';

beforeEach(() => {
  _initTestDatabase();
});

// --- storeChatMetadata ---

describe('storeChatMetadata', () => {
  it('stores chat with JID as default name', () => {
    storeChatMetadata('group@g.us', '2024-01-01T00:00:00.000Z');
    const chats = getAllChats();
    expect(chats).toHaveLength(1);
    expect(chats[0].jid).toBe('group@g.us');
    expect(chats[0].name).toBe('group@g.us');
  });

  it('stores chat with explicit name', () => {
    storeChatMetadata('group@g.us', '2024-01-01T00:00:00.000Z', 'My Group');
    const chats = getAllChats();
    expect(chats[0].name).toBe('My Group');
  });

  it('updates name on subsequent call', () => {
    storeChatMetadata('group@g.us', '2024-01-01T00:00:00.000Z');
    storeChatMetadata('group@g.us', '2024-01-01T00:00:01.000Z', 'Updated Name');
    const chats = getAllChats();
    expect(chats).toHaveLength(1);
    expect(chats[0].name).toBe('Updated Name');
  });

  it('preserves newer timestamp on conflict', () => {
    storeChatMetadata('group@g.us', '2024-01-01T00:00:05.000Z');
    storeChatMetadata('group@g.us', '2024-01-01T00:00:01.000Z');
    const chats = getAllChats();
    expect(chats[0].last_message_time).toBe('2024-01-01T00:00:05.000Z');
  });
});

// --- storeGenericMessage ---

describe('storeGenericMessage', () => {
  it('stores and retrieves a message', () => {
    storeChatMetadata('group@g.us', '2024-01-01T00:00:00.000Z');
    storeGenericMessage(
      'msg-1', 'group@g.us', '123@s.net', 'Alice',
      'hello world', '2024-01-01T00:00:01.000Z', false, 'telegram'
    );

    const messages = getMessagesSince('group@g.us', '2024-01-01T00:00:00.000Z', 'Bot');
    expect(messages).toHaveLength(1);
    expect(messages[0].content).toBe('hello world');
    expect(messages[0].sender_name).toBe('Alice');
  });

  it('upserts on duplicate id+chat_jid', () => {
    storeChatMetadata('group@g.us', '2024-01-01T00:00:00.000Z');
    storeGenericMessage(
      'msg-dup', 'group@g.us', '123@s.net', 'Alice',
      'original', '2024-01-01T00:00:01.000Z', false, 'telegram'
    );
    storeGenericMessage(
      'msg-dup', 'group@g.us', '123@s.net', 'Alice',
      'updated', '2024-01-01T00:00:01.000Z', false, 'telegram'
    );

    const messages = getMessagesSince('group@g.us', '2024-01-01T00:00:00.000Z', 'Bot');
    expect(messages).toHaveLength(1);
    expect(messages[0].content).toBe('updated');
  });
});

// --- getMessagesSince ---

describe('getMessagesSince', () => {
  beforeEach(() => {
    storeChatMetadata('group@g.us', '2024-01-01T00:00:00.000Z');
    storeGenericMessage('m1', 'group@g.us', 'alice', 'Alice', 'first', '2024-01-01T00:00:01.000Z', false, 'telegram');
    storeGenericMessage('m2', 'group@g.us', 'bob', 'Bob', 'second', '2024-01-01T00:00:02.000Z', false, 'telegram');
    storeGenericMessage('m3', 'group@g.us', 'bot', 'Bot', 'Bot: reply', '2024-01-01T00:00:03.000Z', true, 'telegram');
    storeGenericMessage('m4', 'group@g.us', 'carol', 'Carol', 'third', '2024-01-01T00:00:04.000Z', false, 'telegram');
  });

  it('returns messages after the given timestamp', () => {
    const msgs = getMessagesSince('group@g.us', '2024-01-01T00:00:02.000Z', 'Bot');
    // m3 is filtered by prefix, m4 remains
    expect(msgs).toHaveLength(1);
    expect(msgs[0].content).toBe('third');
  });

  it('filters bot messages by content prefix', () => {
    const msgs = getMessagesSince('group@g.us', '2024-01-01T00:00:00.000Z', 'Bot');
    const botMsgs = msgs.filter(m => m.content.startsWith('Bot:'));
    expect(botMsgs).toHaveLength(0);
  });
});

// --- getNewMessages ---

describe('getNewMessages', () => {
  beforeEach(() => {
    storeChatMetadata('g1@g.us', '2024-01-01T00:00:00.000Z');
    storeChatMetadata('g2@g.us', '2024-01-01T00:00:00.000Z');
    storeGenericMessage('a1', 'g1@g.us', 'user', 'User', 'g1 msg1', '2024-01-01T00:00:01.000Z', false, 'telegram');
    storeGenericMessage('a2', 'g2@g.us', 'user', 'User', 'g2 msg1', '2024-01-01T00:00:02.000Z', false, 'telegram');
    storeGenericMessage('a3', 'g1@g.us', 'bot', 'Bot', 'Bot: reply', '2024-01-01T00:00:03.000Z', true, 'telegram');
    storeGenericMessage('a4', 'g1@g.us', 'user', 'User', 'g1 msg2', '2024-01-01T00:00:04.000Z', false, 'telegram');
  });

  it('returns new messages across multiple groups', () => {
    const { messages, newTimestamp } = getNewMessages(
      ['g1@g.us', 'g2@g.us'], '2024-01-01T00:00:00.000Z', 'Bot'
    );
    expect(messages).toHaveLength(3);
    expect(newTimestamp).toBe('2024-01-01T00:00:04.000Z');
  });

  it('filters by timestamp', () => {
    const { messages } = getNewMessages(
      ['g1@g.us', 'g2@g.us'], '2024-01-01T00:00:02.000Z', 'Bot'
    );
    expect(messages).toHaveLength(1);
    expect(messages[0].content).toBe('g1 msg2');
  });

  it('returns empty for no groups', () => {
    const { messages, newTimestamp } = getNewMessages([], '', 'Bot');
    expect(messages).toHaveLength(0);
    expect(newTimestamp).toBe('');
  });
});

// --- Task CRUD ---

describe('task CRUD', () => {
  const baseTask = {
    group_folder: 'main',
    chat_jid: 'group@g.us',
    prompt: 'do something',
    schedule_type: 'once' as const,
    schedule_value: '2024-06-01T00:00:00.000Z',
    context_mode: 'group' as const,
    next_run: '2024-06-01T00:00:00.000Z',
    status: 'active' as const,
    created_at: '2024-01-01T00:00:00.000Z',
  };

  it('creates and retrieves a task', () => {
    createTask({ ...baseTask, id: 'task-1' });
    const task = getTaskById('task-1');
    expect(task).toBeDefined();
    expect(task!.prompt).toBe('do something');
    expect(task!.status).toBe('active');
  });

  it('updates task fields', () => {
    createTask({ ...baseTask, id: 'task-2' });
    updateTask('task-2', { status: 'paused', prompt: 'updated' });
    const task = getTaskById('task-2');
    expect(task!.status).toBe('paused');
    expect(task!.prompt).toBe('updated');
  });

  it('deletes a task and its run logs', () => {
    createTask({ ...baseTask, id: 'task-3' });
    logTaskRun({
      task_id: 'task-3', run_at: '2024-01-01T00:00:00.000Z',
      duration_ms: 100, status: 'success', result: 'ok', error: null,
    });
    deleteTask('task-3');
    expect(getTaskById('task-3')).toBeUndefined();
    expect(getTaskRunLogs('task-3')).toHaveLength(0);
  });

  it('getTasksForGroup filters by group', () => {
    createTask({ ...baseTask, id: 'task-a', group_folder: 'group-1' });
    createTask({ ...baseTask, id: 'task-b', group_folder: 'group-2' });
    createTask({ ...baseTask, id: 'task-c', group_folder: 'group-1' });
    expect(getTasksForGroup('group-1')).toHaveLength(2);
    expect(getTasksForGroup('group-2')).toHaveLength(1);
  });

  it('getAllTasks returns all tasks', () => {
    createTask({ ...baseTask, id: 'task-x' });
    createTask({ ...baseTask, id: 'task-y', group_folder: 'other' });
    expect(getAllTasks()).toHaveLength(2);
  });
});

// --- getDueTasks / claimTask / updateTaskAfterRun ---

describe('getDueTasks and claim lifecycle', () => {
  it('returns only active tasks with next_run in the past', () => {
    const past = new Date(Date.now() - 60_000).toISOString();
    const future = new Date(Date.now() + 60_000).toISOString();

    createTask({
      id: 'due', group_folder: 'g', chat_jid: 'c', prompt: 'p',
      schedule_type: 'cron', schedule_value: '0 * * * *', context_mode: 'group',
      next_run: past, status: 'active', created_at: past,
    });
    createTask({
      id: 'future', group_folder: 'g', chat_jid: 'c', prompt: 'p',
      schedule_type: 'cron', schedule_value: '0 * * * *', context_mode: 'group',
      next_run: future, status: 'active', created_at: past,
    });
    createTask({
      id: 'paused', group_folder: 'g', chat_jid: 'c', prompt: 'p',
      schedule_type: 'cron', schedule_value: '0 * * * *', context_mode: 'group',
      next_run: past, status: 'paused', created_at: past,
    });
    createTask({
      id: 'no-next', group_folder: 'g', chat_jid: 'c', prompt: 'p',
      schedule_type: 'once', schedule_value: '', context_mode: 'group',
      next_run: null, status: 'active', created_at: past,
    });

    const due = getDueTasks();
    expect(due).toHaveLength(1);
    expect(due[0].id).toBe('due');
  });

  it('updateTaskAfterRun marks once tasks as completed', () => {
    const past = new Date(Date.now() - 60_000).toISOString();
    createTask({
      id: 'once-task', group_folder: 'g', chat_jid: 'c', prompt: 'p',
      schedule_type: 'once', schedule_value: '', context_mode: 'group',
      next_run: past, status: 'active', created_at: past,
    });

    updateTaskAfterRun('once-task', null, 'Done');
    const task = getTaskById('once-task');
    expect(task!.status).toBe('completed');
    expect(task!.next_run).toBeNull();
    expect(task!.last_result).toBe('Done');
  });

  it('updateTaskAfterRun keeps recurring tasks active', () => {
    const past = new Date(Date.now() - 60_000).toISOString();
    const future = new Date(Date.now() + 3_600_000).toISOString();
    createTask({
      id: 'recurring', group_folder: 'g', chat_jid: 'c', prompt: 'p',
      schedule_type: 'cron', schedule_value: '0 * * * *', context_mode: 'group',
      next_run: past, status: 'active', created_at: past,
    });

    updateTaskAfterRun('recurring', future, 'OK');
    const task = getTaskById('recurring');
    expect(task!.status).toBe('active');
    expect(task!.next_run).toBe(future);
  });
});

// --- Task run logs ---

describe('task run logs', () => {
  it('logs runs and retrieves them in reverse chronological order', () => {
    createTask({
      id: 'log-task', group_folder: 'g', chat_jid: 'c', prompt: 'p',
      schedule_type: 'cron', schedule_value: '0 * * * *', context_mode: 'group',
      next_run: null, status: 'active', created_at: '2024-01-01T00:00:00.000Z',
    });

    logTaskRun({ task_id: 'log-task', run_at: '2024-01-01T01:00:00.000Z', duration_ms: 100, status: 'success', result: 'first', error: null });
    logTaskRun({ task_id: 'log-task', run_at: '2024-01-01T02:00:00.000Z', duration_ms: 200, status: 'error', result: null, error: 'fail' });

    const logs = getTaskRunLogs('log-task');
    expect(logs).toHaveLength(2);
    expect(logs[0].run_at).toBe('2024-01-01T02:00:00.000Z'); // most recent first
    expect(logs[0].status).toBe('error');
    expect(logs[1].status).toBe('success');
  });

  it('respects limit parameter', () => {
    createTask({
      id: 'limit-task', group_folder: 'g', chat_jid: 'c', prompt: 'p',
      schedule_type: 'cron', schedule_value: '0 * * * *', context_mode: 'group',
      next_run: null, status: 'active', created_at: '2024-01-01T00:00:00.000Z',
    });

    for (let i = 0; i < 5; i++) {
      logTaskRun({ task_id: 'limit-task', run_at: `2024-01-01T0${i}:00:00.000Z`, duration_ms: 100, status: 'success', result: null, error: null });
    }

    expect(getTaskRunLogs('limit-task', 2)).toHaveLength(2);
  });
});
