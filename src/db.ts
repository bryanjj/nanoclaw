import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import { Channel, NewMessage, ScheduledTask, TaskRunLog } from './types.js';
import { STORE_DIR } from './config.js';

let db: Database.Database;

function createSchema(instance: Database.Database): void {
  instance.exec(`
    CREATE TABLE IF NOT EXISTS chats (
      jid TEXT PRIMARY KEY,
      name TEXT,
      last_message_time TEXT,
      channel TEXT
    );
    CREATE TABLE IF NOT EXISTS messages (
      id TEXT,
      chat_jid TEXT,
      sender TEXT,
      sender_name TEXT,
      content TEXT,
      timestamp TEXT,
      is_from_me INTEGER,
      channel TEXT,
      PRIMARY KEY (id, chat_jid),
      FOREIGN KEY (chat_jid) REFERENCES chats(jid)
    );
    CREATE INDEX IF NOT EXISTS idx_timestamp ON messages(timestamp);

    CREATE TABLE IF NOT EXISTS scheduled_tasks (
      id TEXT PRIMARY KEY,
      group_folder TEXT NOT NULL,
      chat_jid TEXT NOT NULL,
      prompt TEXT NOT NULL,
      schedule_type TEXT NOT NULL,
      schedule_value TEXT NOT NULL,
      context_mode TEXT DEFAULT 'group',
      next_run TEXT,
      last_run TEXT,
      last_result TEXT,
      status TEXT DEFAULT 'active',
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_next_run ON scheduled_tasks(next_run);
    CREATE INDEX IF NOT EXISTS idx_status ON scheduled_tasks(status);

    CREATE TABLE IF NOT EXISTS task_run_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      task_id TEXT NOT NULL,
      run_at TEXT NOT NULL,
      duration_ms INTEGER NOT NULL,
      status TEXT NOT NULL,
      result TEXT,
      error TEXT,
      FOREIGN KEY (task_id) REFERENCES scheduled_tasks(id)
    );
    CREATE INDEX IF NOT EXISTS idx_task_run_logs ON task_run_logs(task_id, run_at);

    CREATE TABLE IF NOT EXISTS thought_sessions (
      id                    TEXT PRIMARY KEY,
      chat_jid              TEXT,
      group_folder          TEXT,
      trigger_type          TEXT,
      trigger_msg_id        TEXT,
      task_id               TEXT,
      trigger_preview       TEXT,
      started_at            TEXT NOT NULL,
      duration_ms           INTEGER,
      block_count           INTEGER DEFAULT 0,
      total_thinking_tokens INTEGER
    );
    CREATE TABLE IF NOT EXISTS thought_blocks (
      id              TEXT PRIMARY KEY,
      session_id      TEXT NOT NULL REFERENCES thought_sessions(id),
      block_index     INTEGER NOT NULL,
      timestamp       TEXT NOT NULL,
      thinking        TEXT NOT NULL,
      thinking_tokens INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_thought_blocks_session ON thought_blocks(session_id, block_index);
    CREATE INDEX IF NOT EXISTS idx_thought_sessions_started ON thought_sessions(started_at);
    CREATE INDEX IF NOT EXISTS idx_thought_sessions_chat ON thought_sessions(chat_jid, started_at);
    CREATE INDEX IF NOT EXISTS idx_thought_sessions_trigger_msg ON thought_sessions(trigger_msg_id);
  `);

  // FTS5 tables
  try {
    instance.exec(`
      CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts
        USING fts5(content, content='messages', content_rowid='rowid');

      CREATE TRIGGER IF NOT EXISTS messages_ai
        AFTER INSERT ON messages BEGIN
          INSERT INTO messages_fts(rowid, content) VALUES (new.rowid, new.content);
        END;

      CREATE TRIGGER IF NOT EXISTS messages_ad
        AFTER DELETE ON messages BEGIN
          INSERT INTO messages_fts(messages_fts, rowid, content) VALUES ('delete', old.rowid, old.content);
        END;

      CREATE TRIGGER IF NOT EXISTS messages_au
        AFTER UPDATE ON messages BEGIN
          INSERT INTO messages_fts(messages_fts, rowid, content) VALUES ('delete', old.rowid, old.content);
          INSERT INTO messages_fts(rowid, content) VALUES (new.rowid, new.content);
        END;

      CREATE VIRTUAL TABLE IF NOT EXISTS thought_blocks_fts
        USING fts5(thinking, content='thought_blocks', content_rowid='rowid');

      CREATE TRIGGER IF NOT EXISTS thought_blocks_ai
        AFTER INSERT ON thought_blocks BEGIN
          INSERT INTO thought_blocks_fts(rowid, thinking) VALUES (new.rowid, new.thinking);
        END;

      CREATE TRIGGER IF NOT EXISTS thought_blocks_ad
        AFTER DELETE ON thought_blocks BEGIN
          INSERT INTO thought_blocks_fts(thought_blocks_fts, rowid, thinking) VALUES ('delete', old.rowid, old.thinking);
        END;

      CREATE TRIGGER IF NOT EXISTS thought_blocks_au
        AFTER UPDATE ON thought_blocks BEGIN
          INSERT INTO thought_blocks_fts(thought_blocks_fts, rowid, thinking) VALUES ('delete', old.rowid, old.thinking);
          INSERT INTO thought_blocks_fts(rowid, thinking) VALUES (new.rowid, new.thinking);
        END;
    `);
  } catch { /* FTS tables already exist */ }
}

export function initDatabase(): void {
  const dbPath = path.join(STORE_DIR, 'messages.db');
  fs.mkdirSync(path.dirname(dbPath), { recursive: true });

  db = new Database(dbPath);
  createSchema(db);

  // Migrations for existing DBs — columns already in createSchema for new DBs
  try { db.exec(`ALTER TABLE messages ADD COLUMN sender_name TEXT`); } catch { /* exists */ }
  try { db.exec(`ALTER TABLE scheduled_tasks ADD COLUMN context_mode TEXT DEFAULT 'group'`); } catch { /* exists */ }
  try { db.exec(`ALTER TABLE messages ADD COLUMN channel TEXT DEFAULT 'telegram'`); } catch { /* exists */ }
  try { db.exec(`ALTER TABLE chats ADD COLUMN channel TEXT DEFAULT 'telegram'`); } catch { /* exists */ }
}

/** @internal — for tests only. Replaces db with a fresh in-memory instance. */
export function _initTestDatabase(): void {
  db = new Database(':memory:');
  createSchema(db);
}

/**
 * Store chat metadata only (no message content).
 * Used for all chats to enable group discovery without storing sensitive content.
 */
export function storeChatMetadata(chatJid: string, timestamp: string, name?: string): void {
  if (name) {
    // Update with name, preserving existing timestamp if newer
    db.prepare(`
      INSERT INTO chats (jid, name, last_message_time) VALUES (?, ?, ?)
      ON CONFLICT(jid) DO UPDATE SET
        name = excluded.name,
        last_message_time = MAX(last_message_time, excluded.last_message_time)
    `).run(chatJid, name, timestamp);
  } else {
    // Update timestamp only, preserve existing name if any
    db.prepare(`
      INSERT INTO chats (jid, name, last_message_time) VALUES (?, ?, ?)
      ON CONFLICT(jid) DO UPDATE SET
        last_message_time = MAX(last_message_time, excluded.last_message_time)
    `).run(chatJid, chatJid, timestamp);
  }
}

export interface ChatInfo {
  jid: string;
  name: string;
  last_message_time: string;
}

/**
 * Get all known chats, ordered by most recent activity.
 */
export function getAllChats(): ChatInfo[] {
  return db.prepare(`
    SELECT jid, name, last_message_time
    FROM chats
    ORDER BY last_message_time DESC
  `).all() as ChatInfo[];
}

/**
 * Store a message with full content.
 */
export function storeGenericMessage(
  msgId: string,
  chatJid: string,
  sender: string,
  senderName: string,
  content: string,
  timestamp: string,
  isFromMe: boolean,
  channel: Channel
): void {
  db.prepare(`INSERT OR REPLACE INTO messages (id, chat_jid, sender, sender_name, content, timestamp, is_from_me, channel) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
    .run(msgId, chatJid, sender, senderName, content, timestamp, isFromMe ? 1 : 0, channel);
}

export function getNewMessages(jids: string[], lastTimestamp: string, botPrefix: string): { messages: NewMessage[]; newTimestamp: string } {
  if (jids.length === 0) return { messages: [], newTimestamp: lastTimestamp };

  const placeholders = jids.map(() => '?').join(',');
  // Filter out bot's own messages by checking content prefix (not is_from_me, since user shares the account)
  const sql = `
    SELECT id, chat_jid, sender, sender_name, content, timestamp, COALESCE(channel, 'telegram') as channel
    FROM messages
    WHERE timestamp > ? AND chat_jid IN (${placeholders}) AND content NOT LIKE ?
    ORDER BY timestamp
  `;

  const rows = db.prepare(sql).all(lastTimestamp, ...jids, `${botPrefix}:%`) as NewMessage[];

  let newTimestamp = lastTimestamp;
  for (const row of rows) {
    if (row.timestamp > newTimestamp) newTimestamp = row.timestamp;
  }

  return { messages: rows, newTimestamp };
}

export function getMessagesSince(chatJid: string, sinceTimestamp: string, botPrefix: string): NewMessage[] {
  // Filter out bot's own messages by checking content prefix
  const sql = `
    SELECT id, chat_jid, sender, sender_name, content, timestamp, COALESCE(channel, 'telegram') as channel
    FROM messages
    WHERE chat_jid = ? AND timestamp > ? AND content NOT LIKE ?
    ORDER BY timestamp
  `;
  return db.prepare(sql).all(chatJid, sinceTimestamp, `${botPrefix}:%`) as NewMessage[];
}

export function createTask(task: Omit<ScheduledTask, 'last_run' | 'last_result'>): void {
  db.prepare(`
    INSERT INTO scheduled_tasks (id, group_folder, chat_jid, prompt, schedule_type, schedule_value, context_mode, next_run, status, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    task.id,
    task.group_folder,
    task.chat_jid,
    task.prompt,
    task.schedule_type,
    task.schedule_value,
    task.context_mode || 'group',
    task.next_run,
    task.status,
    task.created_at
  );
}

export function getTaskById(id: string): ScheduledTask | undefined {
  return db.prepare('SELECT * FROM scheduled_tasks WHERE id = ?').get(id) as ScheduledTask | undefined;
}

export function getTasksForGroup(groupFolder: string): ScheduledTask[] {
  return db.prepare('SELECT * FROM scheduled_tasks WHERE group_folder = ? ORDER BY created_at DESC').all(groupFolder) as ScheduledTask[];
}

export function getAllTasks(): ScheduledTask[] {
  return db.prepare('SELECT * FROM scheduled_tasks ORDER BY created_at DESC').all() as ScheduledTask[];
}

export function updateTask(id: string, updates: Partial<Pick<ScheduledTask, 'prompt' | 'schedule_type' | 'schedule_value' | 'next_run' | 'status'>>): void {
  const fields: string[] = [];
  const values: unknown[] = [];

  if (updates.prompt !== undefined) { fields.push('prompt = ?'); values.push(updates.prompt); }
  if (updates.schedule_type !== undefined) { fields.push('schedule_type = ?'); values.push(updates.schedule_type); }
  if (updates.schedule_value !== undefined) { fields.push('schedule_value = ?'); values.push(updates.schedule_value); }
  if (updates.next_run !== undefined) { fields.push('next_run = ?'); values.push(updates.next_run); }
  if (updates.status !== undefined) { fields.push('status = ?'); values.push(updates.status); }

  if (fields.length === 0) return;

  values.push(id);
  db.prepare(`UPDATE scheduled_tasks SET ${fields.join(', ')} WHERE id = ?`).run(...values);
}

export function deleteTask(id: string): void {
  // Delete child records first (FK constraint)
  db.prepare('DELETE FROM task_run_logs WHERE task_id = ?').run(id);
  db.prepare('DELETE FROM scheduled_tasks WHERE id = ?').run(id);
}

export function getDueTasks(): ScheduledTask[] {
  const now = new Date().toISOString();
  return db.prepare(`
    SELECT * FROM scheduled_tasks
    WHERE status = 'active' AND next_run IS NOT NULL AND next_run <= ?
    ORDER BY next_run
  `).all(now) as ScheduledTask[];
}

export function claimTask(id: string, nextRun: string | null): void {
  db.prepare(`
    UPDATE scheduled_tasks
    SET next_run = ?, last_run = ?
    WHERE id = ?
  `).run(nextRun, new Date().toISOString(), id);
}

export function updateTaskAfterRun(id: string, nextRun: string | null, lastResult: string): void {
  const now = new Date().toISOString();
  db.prepare(`
    UPDATE scheduled_tasks
    SET next_run = ?, last_run = ?, last_result = ?, status = CASE WHEN ? IS NULL THEN 'completed' ELSE status END
    WHERE id = ?
  `).run(nextRun, now, lastResult, nextRun, id);
}

export function logTaskRun(log: TaskRunLog): void {
  db.prepare(`
    INSERT INTO task_run_logs (task_id, run_at, duration_ms, status, result, error)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(log.task_id, log.run_at, log.duration_ms, log.status, log.result, log.error);
}

export function getTaskRunLogs(taskId: string, limit = 10): TaskRunLog[] {
  return db.prepare(`
    SELECT task_id, run_at, duration_ms, status, result, error
    FROM task_run_logs
    WHERE task_id = ?
    ORDER BY run_at DESC
    LIMIT ?
  `).all(taskId, limit) as TaskRunLog[];
}

// --- Thought history ---

export interface ThoughtSessionInput {
  id: string;
  chatJid: string;
  groupFolder: string;
  triggerType: string;
  triggerMsgId?: string;
  taskId?: string;
  triggerPreview: string;
  startedAt: string;
}

export function createThoughtSession(session: ThoughtSessionInput): void {
  db.prepare(`
    INSERT OR IGNORE INTO thought_sessions
      (id, chat_jid, group_folder, trigger_type, trigger_msg_id, task_id, trigger_preview, started_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    session.id,
    session.chatJid,
    session.groupFolder,
    session.triggerType,
    session.triggerMsgId ?? null,
    session.taskId ?? null,
    session.triggerPreview.slice(0, 200),
    session.startedAt
  );
}

export function insertThoughtBlock(block: {
  id: string;
  sessionId: string;
  blockIndex: number;
  timestamp: string;
  thinking: string;
  thinkingTokens?: number;
}): void {
  db.prepare(`
    INSERT OR IGNORE INTO thought_blocks
      (id, session_id, block_index, timestamp, thinking, thinking_tokens)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(block.id, block.sessionId, block.blockIndex, block.timestamp, block.thinking, block.thinkingTokens ?? null);
}

export function finalizeThoughtSession(id: string, durationMs: number, blockCount: number, totalTokens: number | null): void {
  db.prepare(`
    UPDATE thought_sessions
    SET duration_ms = ?, block_count = ?, total_thinking_tokens = ?
    WHERE id = ?
  `).run(durationMs, blockCount, totalTokens, id);
}

export function deleteThoughtSession(id: string): void {
  db.prepare('DELETE FROM thought_blocks WHERE session_id = ?').run(id);
  db.prepare('DELETE FROM thought_sessions WHERE id = ?').run(id);
}
