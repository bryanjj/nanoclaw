import 'dotenv/config';
import pino from 'pino';
import { exec, execSync } from 'child_process';
import fs from 'fs';
import path from 'path';

import {
  ASSISTANT_NAME,
  POLL_INTERVAL,
  STORE_DIR,
  DATA_DIR,
  TRIGGER_PATTERN,
  MAIN_GROUP_FOLDER,
  IPC_POLL_INTERVAL,
  TIMEZONE,
  DASHBOARD_ENABLED
} from './config.js';
import { RegisteredGroup, NewMessage } from './types.js';
import crypto from 'crypto';
import { initDatabase, storeGenericMessage, getNewMessages, getMessagesSince, getAllTasks, getTaskById, getAllChats, getRecentMessages, getRecentThoughts } from './db.js';
import { startSchedulerLoop } from './task-scheduler.js';
import { runContainerAgent, writeTasksSnapshot, writeGroupsSnapshot, AvailableGroup } from './container-runner.js';
import { loadJson, saveJson } from './utils.js';
import { isValidGroupFolder, resolveGroupFolderPath } from './group-folder.js';
import { GroupQueue } from './group-queue.js';
import './channels/index.js';
import { ChannelInstance, getRegisteredChannelNames, getChannelFactory } from './channels/registry.js';
import { startDashboardServer } from './dashboard-server.js';
import { dashboardEvents } from './dashboard-events.js';

const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  transport: { target: 'pino-pretty', options: { colorize: true } }
});

let lastTimestamp = '';
let registeredGroups: Record<string, RegisteredGroup> = {};
let lastAgentTimestamp: Record<string, string> = {};
const groupQueue = new GroupQueue();
const channels: ChannelInstance[] = [];

function findChannel(jid: string): ChannelInstance | undefined {
  return channels.find(c => c.ownsJid(jid));
}

async function setTyping(jid: string, isTyping: boolean): Promise<void> {
  await findChannel(jid)?.setTyping?.(jid, isTyping);
}

function loadState(): void {
  const statePath = path.join(DATA_DIR, 'router_state.json');
  const state = loadJson<{ last_timestamp?: string; last_agent_timestamp?: Record<string, string> }>(statePath, {});
  lastTimestamp = state.last_timestamp || '';
  lastAgentTimestamp = state.last_agent_timestamp || {};
  registeredGroups = loadJson(path.join(DATA_DIR, 'registered_groups.json'), {});

  // Ensure all groups have a channel set
  for (const [, group] of Object.entries(registeredGroups)) {
    if (!group.channel) {
      group.channel = 'telegram';
    }
  }

  logger.info({ groupCount: Object.keys(registeredGroups).length }, 'State loaded');
}

function saveState(): void {
  saveJson(path.join(DATA_DIR, 'router_state.json'), { last_timestamp: lastTimestamp, last_agent_timestamp: lastAgentTimestamp });
}

function registerGroup(jid: string, group: RegisteredGroup): void {
  let groupDir: string;
  try {
    groupDir = resolveGroupFolderPath(group.folder);
  } catch (err) {
    logger.warn({ jid, folder: group.folder, err }, 'Rejecting group registration with invalid folder');
    return;
  }

  if (!group.channel) {
    group.channel = findChannel(jid)?.name || 'telegram';
  }
  registeredGroups[jid] = group;
  saveJson(path.join(DATA_DIR, 'registered_groups.json'), registeredGroups);

  // Create group folder
  fs.mkdirSync(path.join(groupDir, 'logs'), { recursive: true });

  logger.info({ jid, name: group.name, folder: group.folder, channel: group.channel }, 'Group registered');
}

/**
 * Get available groups list for the agent.
 * Returns groups ordered by most recent activity.
 */
function getAvailableGroups(): AvailableGroup[] {
  const chats = getAllChats();
  const registeredJids = new Set(Object.keys(registeredGroups));

  return chats
    .filter(c => c.jid !== '__group_sync__' && channels.some(ch => ch.ownsJid(c.jid)))
    .map(c => ({
      jid: c.jid,
      name: c.name,
      lastActivity: c.last_message_time,
      isRegistered: registeredJids.has(c.jid)
    }));
}

async function processMessage(msg: NewMessage): Promise<void> {
  const group = registeredGroups[msg.chat_jid];
  if (!group) return;

  const content = msg.content.trim();
  const isMainGroup = group.folder === MAIN_GROUP_FOLDER;

  // Main group responds to all messages; other groups require trigger prefix
  if (!isMainGroup && !TRIGGER_PATTERN.test(content)) return;

  // Get all messages since last agent interaction so the session has full context
  const sinceTimestamp = lastAgentTimestamp[msg.chat_jid] || '';
  const missedMessages = getMessagesSince(msg.chat_jid, sinceTimestamp, ASSISTANT_NAME);

  // All messages already handled by a prior queued container run — skip
  if (missedMessages.length === 0) {
    logger.info({ group: group.name }, 'No new messages since last agent run, skipping');
    return;
  }

  // Track the latest message timestamp so subsequent queued runs don't re-process
  const latestTimestamp = missedMessages[missedMessages.length - 1].timestamp;

  const lines = missedMessages.map(m => {
    return `<message sender="${escapeXml(m.sender_name)}" time="${m.timestamp}">${escapeXml(m.content)}</message>`;
  });
  const prompt = `<messages>\n${lines.join('\n')}\n</messages>`;

  logger.info({ group: group.name, messageCount: missedMessages.length }, 'Processing message');

  // Emit message received event for dashboard
  dashboardEvents.emitDashboard({
    type: 'message_received',
    timestamp: new Date().toISOString(),
    groupFolder: group.folder,
    chatJid: msg.chat_jid,
    data: { sender: msg.sender_name, preview: msg.content }
  });

  await setTyping(msg.chat_jid, true);
  const { response, messagesSent } = await runAgent(group, prompt, msg.chat_jid, msg.id);
  await setTyping(msg.chat_jid, false);

  // Update timestamp to the latest gathered message (not just the trigger)
  // This prevents subsequent queued containers from re-processing these messages
  if (response || messagesSent) {
    lastAgentTimestamp[msg.chat_jid] = latestTimestamp;
  }
}

const CONTEXT_HISTORY_THOUGHT_CAP = 20000;
const CONTEXT_HISTORY_MESSAGE_CAP = 30000;

function escapeXml(s: string): string {
  return s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function buildContextPrefix(chatJid: string, groupFolder: string): string {
  const sections: string[] = [];

  // Recent messages: fetch newest-first, accumulate until cap, reverse to chronological
  const rawMessages = getRecentMessages(chatJid, 500);
  if (rawMessages.length > 0) {
    let totalChars = 0;
    const kept: typeof rawMessages = [];
    for (const m of rawMessages) {
      if (totalChars + m.content.length > CONTEXT_HISTORY_MESSAGE_CAP) break;
      totalChars += m.content.length;
      kept.push(m);
    }
    kept.reverse(); // chronological order
    const msgLines = kept.map(m =>
      `<message sender="${escapeXml(m.sender_name)}" time="${m.timestamp}">${escapeXml(m.content)}</message>`
    );
    sections.push(`<recent_messages>\n${msgLines.join('\n')}\n</recent_messages>`);
  }

  // Recent thoughts: fetch newest-first, accumulate until cap, reverse to chronological
  const rawThoughts = getRecentThoughts(groupFolder, 100);
  if (rawThoughts.length > 0) {
    let totalChars = 0;
    const kept: typeof rawThoughts = [];
    for (const session of rawThoughts) {
      const thinking = session.blocks.map(b => b.thinking).join('\n');
      if (totalChars + thinking.length > CONTEXT_HISTORY_THOUGHT_CAP) break;
      totalChars += thinking.length;
      kept.push(session);
    }
    kept.reverse(); // chronological order
    const thoughtLines = kept.map(session => {
      const thinking = session.blocks.map(b => b.thinking).join('\n');
      return `<thought trigger="${escapeXml(session.triggerType)}" time="${session.startedAt}">\n${thinking}\n</thought>`;
    });
    sections.push(`<recent_thoughts>\n${thoughtLines.join('\n')}\n</recent_thoughts>`);
  }

  if (sections.length === 0) return '';
  return `<context_history>\n${sections.join('\n\n')}\n</context_history>\n\n`;
}

async function runAgent(group: RegisteredGroup, prompt: string, chatJid: string, triggerMsgId?: string): Promise<{ response: string | null; messagesSent: number }> {
  const isMain = group.folder === MAIN_GROUP_FOLDER;

  // Update tasks snapshot for container to read (filtered by group)
  const tasks = getAllTasks();
  writeTasksSnapshot(group.folder, isMain, tasks.map(t => ({
    id: t.id,
    groupFolder: t.group_folder,
    prompt: t.prompt,
    schedule_type: t.schedule_type,
    schedule_value: t.schedule_value,
    status: t.status,
    next_run: t.next_run
  })));

  // Update available groups snapshot (main group only can see all groups)
  const availableGroups = getAvailableGroups();
  writeGroupsSnapshot(group.folder, isMain, availableGroups, new Set(Object.keys(registeredGroups)));

  // Prepend context history (recent messages + thoughts) for fresh session awareness
  const contextPrefix = buildContextPrefix(chatJid, group.folder);
  const fullPrompt = contextPrefix + prompt;

  try {
    const output = await runContainerAgent(group, {
      prompt: fullPrompt,
      groupFolder: group.folder,
      chatJid,
      isMain,
      triggerType: 'user_message',
      triggerMsgId
    });

    if (output.status === 'error') {
      logger.error({ group: group.name, error: output.error }, 'Container agent error');
      return { response: null, messagesSent: output.messagesSent || 0 };
    }

    return { response: output.result, messagesSent: output.messagesSent || 0 };
  } catch (err) {
    logger.error({ group: group.name, err }, 'Agent error');
    return { response: null, messagesSent: 0 };
  }
}

async function sendMessage(jid: string, text: string): Promise<void> {
  // Emit message sent event for dashboard
  dashboardEvents.emitDashboard({
    type: 'message_sent',
    timestamp: new Date().toISOString(),
    chatJid: jid,
    data: { length: text.length }
  });

  const channel = findChannel(jid);
  if (channel) {
    await channel.sendMessage(jid, text);
  } else {
    logger.warn({ jid }, 'No channel found for JID');
  }

  // Store outgoing message in history
  storeGenericMessage(crypto.randomUUID(), jid, ASSISTANT_NAME, ASSISTANT_NAME, text, new Date().toISOString(), true, channel?.name || 'unknown');
}

async function sendPhoto(jid: string, photoPath: string, caption?: string): Promise<void> {
  logger.info({ jid, photoPath, caption }, 'sendPhoto called');

  // Check if file exists
  if (!fs.existsSync(photoPath)) {
    logger.error({ jid, photoPath }, 'Photo file does not exist');
    return;
  }

  logger.info({ jid, photoPath, fileSize: fs.statSync(photoPath).size }, 'Photo file exists');

  // Emit message sent event for dashboard
  dashboardEvents.emitDashboard({
    type: 'message_sent',
    timestamp: new Date().toISOString(),
    chatJid: jid,
    data: { photo: photoPath, caption }
  });

  const channel = findChannel(jid);
  if (channel) {
    await channel.sendPhoto(jid, photoPath, caption);
    logger.info({ jid, photoPath, channel: channel.name }, 'Photo sent');
  } else {
    logger.warn({ jid, photoPath }, 'No channel found for JID');
  }

  // Store outgoing photo in history
  const content = caption ? `[photo] ${caption}` : '[photo]';
  storeGenericMessage(crypto.randomUUID(), jid, ASSISTANT_NAME, ASSISTANT_NAME, content, new Date().toISOString(), true, channel?.name || 'unknown');
}

function startIpcWatcher(): void {
  const ipcBaseDir = path.join(DATA_DIR, 'ipc');
  fs.mkdirSync(ipcBaseDir, { recursive: true });

  const processIpcFiles = async () => {
    // Scan all group IPC directories (identity determined by directory)
    let groupFolders: string[];
    try {
      groupFolders = fs.readdirSync(ipcBaseDir).filter(f => {
        const stat = fs.statSync(path.join(ipcBaseDir, f));
        return stat.isDirectory() && f !== 'errors';
      });
    } catch (err) {
      logger.error({ err }, 'Error reading IPC base directory');
      setTimeout(processIpcFiles, IPC_POLL_INTERVAL);
      return;
    }

    for (const sourceGroup of groupFolders) {
      const isMain = sourceGroup === MAIN_GROUP_FOLDER;
      const messagesDir = path.join(ipcBaseDir, sourceGroup, 'messages');
      const tasksDir = path.join(ipcBaseDir, sourceGroup, 'tasks');

      // Process messages from this group's IPC directory
      try {
        if (fs.existsSync(messagesDir)) {
          const messageFiles = fs.readdirSync(messagesDir).filter(f => f.endsWith('.json'));
          for (const file of messageFiles) {
            const filePath = path.join(messagesDir, file);
            try {
              const data = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
              if (data.type === 'message' && data.chatJid && data.text) {
                // Authorization: verify this group can send to this chatJid
                const targetGroup = registeredGroups[data.chatJid];
                if (isMain || (targetGroup && targetGroup.folder === sourceGroup)) {
                  await sendMessage(data.chatJid, data.text);
                  logger.info({ chatJid: data.chatJid, sourceGroup }, 'IPC message sent');
                } else {
                  logger.warn({ chatJid: data.chatJid, sourceGroup }, 'Unauthorized IPC message attempt blocked');
                }
              } else if (data.type === 'photo' && data.chatJid && data.photoPath) {
                logger.info({ data, sourceGroup }, 'Processing photo IPC message');
                // Authorization: verify this group can send to this chatJid
                const targetGroup = registeredGroups[data.chatJid];
                if (isMain || (targetGroup && targetGroup.folder === sourceGroup)) {
                  logger.info({ chatJid: data.chatJid, photoPath: data.photoPath, caption: data.caption }, 'Calling sendPhoto');
                  await sendPhoto(data.chatJid, data.photoPath, data.caption);
                  logger.info({ chatJid: data.chatJid, sourceGroup, photoPath: data.photoPath }, 'IPC photo sent');
                } else {
                  logger.warn({ chatJid: data.chatJid, sourceGroup }, 'Unauthorized IPC photo attempt blocked');
                }
              } else if (data.type === 'reaction' && data.chatJid && data.messageId && data.emoji) {
                // Authorization: verify this group can send to this chatJid
                const targetGroup = registeredGroups[data.chatJid];
                if (isMain || (targetGroup && targetGroup.folder === sourceGroup)) {
                  await findChannel(data.chatJid)?.sendReaction?.(data.chatJid, data.messageId, data.emoji);
                  logger.info({ chatJid: data.chatJid, messageId: data.messageId, emoji: data.emoji, sourceGroup }, 'IPC reaction sent');
                } else {
                  logger.warn({ chatJid: data.chatJid, sourceGroup }, 'Unauthorized IPC reaction attempt blocked');
                }
              }
              fs.unlinkSync(filePath);
            } catch (err) {
              logger.error({ file, sourceGroup, err }, 'Error processing IPC message');
              const errorDir = path.join(ipcBaseDir, 'errors');
              fs.mkdirSync(errorDir, { recursive: true });
              fs.renameSync(filePath, path.join(errorDir, `${sourceGroup}-${file}`));
            }
          }
        }
      } catch (err) {
        logger.error({ err, sourceGroup }, 'Error reading IPC messages directory');
      }

      // Process tasks from this group's IPC directory
      try {
        if (fs.existsSync(tasksDir)) {
          const taskFiles = fs.readdirSync(tasksDir).filter(f => f.endsWith('.json'));
          for (const file of taskFiles) {
            const filePath = path.join(tasksDir, file);
            try {
              const data = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
              // Pass source group identity to processTaskIpc for authorization
              await processTaskIpc(data, sourceGroup, isMain);
              fs.unlinkSync(filePath);
            } catch (err) {
              logger.error({ file, sourceGroup, err }, 'Error processing IPC task');
              const errorDir = path.join(ipcBaseDir, 'errors');
              fs.mkdirSync(errorDir, { recursive: true });
              fs.renameSync(filePath, path.join(errorDir, `${sourceGroup}-${file}`));
            }
          }
        }
      } catch (err) {
        logger.error({ err, sourceGroup }, 'Error reading IPC tasks directory');
      }
    }

    setTimeout(processIpcFiles, IPC_POLL_INTERVAL);
  };

  processIpcFiles();
  logger.info('IPC watcher started (per-group namespaces)');
}

async function processTaskIpc(
  data: {
    type: string;
    taskId?: string;
    prompt?: string;
    schedule_type?: string;
    schedule_value?: string;
    context_mode?: string;
    groupFolder?: string;
    chatJid?: string;
    // For register_group
    jid?: string;
    name?: string;
    folder?: string;
    trigger?: string;
    containerConfig?: RegisteredGroup['containerConfig'];
  },
  sourceGroup: string,  // Verified identity from IPC directory
  isMain: boolean       // Verified from directory path
): Promise<void> {
  // Import db functions dynamically to avoid circular deps
  const { createTask, updateTask, deleteTask, getTaskById: getTask } = await import('./db.js');
  const { CronExpressionParser } = await import('cron-parser');

  switch (data.type) {
    case 'schedule_task':
      if (data.prompt && data.schedule_type && data.schedule_value && data.groupFolder) {
        // Authorization: non-main groups can only schedule for themselves
        const targetGroup = data.groupFolder;
        if (!isMain && targetGroup !== sourceGroup) {
          logger.warn({ sourceGroup, targetGroup }, 'Unauthorized schedule_task attempt blocked');
          break;
        }

        // Resolve the correct JID for the target group (don't trust IPC payload)
        const targetJid = Object.entries(registeredGroups).find(
          ([, group]) => group.folder === targetGroup
        )?.[0];

        if (!targetJid) {
          logger.warn({ targetGroup }, 'Cannot schedule task: target group not registered');
          break;
        }

        const scheduleType = data.schedule_type as 'cron' | 'interval' | 'once';

        let nextRun: string | null = null;
        if (scheduleType === 'cron') {
          try {
            const interval = CronExpressionParser.parse(data.schedule_value, { tz: 'UTC' });
            nextRun = interval.next().toISOString();
          } catch {
            logger.warn({ scheduleValue: data.schedule_value }, 'Invalid cron expression');
            break;
          }
        } else if (scheduleType === 'interval') {
          const ms = parseInt(data.schedule_value, 10);
          if (isNaN(ms) || ms <= 0) {
            logger.warn({ scheduleValue: data.schedule_value }, 'Invalid interval');
            break;
          }
          nextRun = new Date(Date.now() + ms).toISOString();
        } else if (scheduleType === 'once') {
          // Always interpret as UTC - add Z if missing
          let utcValue = data.schedule_value;
          if (!utcValue.endsWith('Z') && !utcValue.includes('+') && !utcValue.includes('-', 10)) {
            utcValue = utcValue + 'Z';
          }
          const scheduled = new Date(utcValue);
          if (isNaN(scheduled.getTime())) {
            logger.warn({ scheduleValue: data.schedule_value }, 'Invalid timestamp');
            break;
          }
          nextRun = scheduled.toISOString();
        }

        const taskId = `task-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        const contextMode = 'group';
        createTask({
          id: taskId,
          group_folder: targetGroup,
          chat_jid: targetJid,
          prompt: data.prompt,
          schedule_type: scheduleType,
          schedule_value: data.schedule_value,
          context_mode: contextMode,
          next_run: nextRun,
          status: 'active',
          created_at: new Date().toISOString()
        });
        logger.info({ taskId, sourceGroup, targetGroup, contextMode }, 'Task created via IPC');
      }
      break;

    case 'pause_task':
      if (data.taskId) {
        const task = getTask(data.taskId);
        if (task && (isMain || task.group_folder === sourceGroup)) {
          updateTask(data.taskId, { status: 'paused' });
          logger.info({ taskId: data.taskId, sourceGroup }, 'Task paused via IPC');
        } else {
          logger.warn({ taskId: data.taskId, sourceGroup }, 'Unauthorized task pause attempt');
        }
      }
      break;

    case 'resume_task':
      if (data.taskId) {
        const task = getTask(data.taskId);
        if (task && (isMain || task.group_folder === sourceGroup)) {
          updateTask(data.taskId, { status: 'active' });
          logger.info({ taskId: data.taskId, sourceGroup }, 'Task resumed via IPC');
        } else {
          logger.warn({ taskId: data.taskId, sourceGroup }, 'Unauthorized task resume attempt');
        }
      }
      break;

    case 'cancel_task':
      if (data.taskId) {
        const task = getTask(data.taskId);
        if (task && (isMain || task.group_folder === sourceGroup)) {
          deleteTask(data.taskId);
          logger.info({ taskId: data.taskId, sourceGroup }, 'Task cancelled via IPC');
        } else {
          logger.warn({ taskId: data.taskId, sourceGroup }, 'Unauthorized task cancel attempt');
        }
      }
      break;

    case 'refresh_groups':
      if (isMain) {
        logger.info({ sourceGroup }, 'Group list refresh requested via IPC');
        const availableGroups = getAvailableGroups();
        const { writeGroupsSnapshot: writeGroups } = await import('./container-runner.js');
        writeGroups(sourceGroup, true, availableGroups, new Set(Object.keys(registeredGroups)));
      } else {
        logger.warn({ sourceGroup }, 'Unauthorized refresh_groups attempt blocked');
      }
      break;

    case 'register_group':
      // Only main group can register new groups
      if (!isMain) {
        logger.warn({ sourceGroup }, 'Unauthorized register_group attempt blocked');
        break;
      }
      if (data.jid && data.name && data.folder && data.trigger) {
        if (!isValidGroupFolder(data.folder)) {
          logger.warn({ sourceGroup, folder: data.folder }, 'Invalid register_group request - unsafe folder name');
          break;
        }
        const channel = findChannel(data.jid)?.name || 'telegram';
        registerGroup(data.jid, {
          name: data.name,
          folder: data.folder,
          trigger: data.trigger,
          added_at: new Date().toISOString(),
          channel,
          containerConfig: data.containerConfig
        });
      } else {
        logger.warn({ data }, 'Invalid register_group request - missing required fields');
      }
      break;

    default:
      logger.warn({ type: data.type }, 'Unknown IPC task type');
  }
}

async function startMessageLoop(): Promise<void> {
  const channelNames = channels.map(c => c.name).join(', ') || 'none';
  logger.info(`NanoClaw running (trigger: @${ASSISTANT_NAME}, channels: ${channelNames})`);

  while (true) {
    try {
      const jids = Object.keys(registeredGroups);
      const { messages } = getNewMessages(jids, lastTimestamp, ASSISTANT_NAME);

      if (messages.length > 0) logger.info({ count: messages.length }, 'New messages');

      // Deduplicate: only enqueue the LAST message per group per poll cycle.
      // processMessage already gathers all missed messages since the last agent
      // interaction, so processing each message individually would cause
      // near-duplicate agent runs.
      const lastPerGroup = new Map<string, NewMessage>();
      for (const msg of messages) {
        const group = registeredGroups[msg.chat_jid];
        if (group && msg.sender_name !== ASSISTANT_NAME) {
          lastPerGroup.set(group.folder, msg);
        }
        // Advance polling cursor so we don't re-discover this message
        lastTimestamp = msg.timestamp;
        saveState();
      }

      for (const [folder, msg] of lastPerGroup) {
        groupQueue.enqueue(folder, async () => {
          try {
            await processMessage(msg);
          } catch (err) {
            logger.error({ err, msg: msg.id }, 'Error processing message');
          }
        });
      }
    } catch (err) {
      logger.error({ err }, 'Error in message loop');
    }
    await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL));
  }
}

function ensureContainerSystemRunning(): void {
  try {
    execSync('container system status', { stdio: 'pipe' });
    logger.debug('Apple Container system already running');
  } catch {
    logger.info('Starting Apple Container system...');
    try {
      execSync('container system start', { stdio: 'pipe', timeout: 30000 });
      logger.info('Apple Container system started');
    } catch (err) {
      logger.error({ err }, 'Failed to start Apple Container system');
      console.error('\n╔════════════════════════════════════════════════════════════════╗');
      console.error('║  FATAL: Apple Container system failed to start                 ║');
      console.error('║                                                                ║');
      console.error('║  Agents cannot run without Apple Container. To fix:           ║');
      console.error('║  1. Install from: https://github.com/apple/container/releases ║');
      console.error('║  2. Run: container system start                               ║');
      console.error('║  3. Restart NanoClaw                                          ║');
      console.error('╚════════════════════════════════════════════════════════════════╝\n');
      throw new Error('Apple Container system is required but failed to start');
    }
  }
}

function initChannels(): void {
  const callbacks = {
    onMessage: (chatJid: string, message: { id: string; sender: string; senderName: string; content: string; timestamp: string }) => {
      logger.debug({ chatJid, messageId: message.id }, 'Channel message received');
    },
    getRegisteredGroups: () => registeredGroups
  };

  for (const name of getRegisteredChannelNames()) {
    const factory = getChannelFactory(name)!;
    const channel = factory(callbacks);
    if (channel) {
      channels.push(channel);
      logger.info({ channel: name }, 'Channel enabled');
    } else {
      logger.warn({ channel: name }, 'Channel installed but credentials missing — skipping');
    }
  }
}

const PID_FILE = path.join(DATA_DIR, 'nanoclaw.pid');

function acquirePidLock(): void {
  fs.mkdirSync(DATA_DIR, { recursive: true });

  if (fs.existsSync(PID_FILE)) {
    const existingPid = parseInt(fs.readFileSync(PID_FILE, 'utf-8').trim(), 10);
    if (!isNaN(existingPid)) {
      try {
        // Signal 0 checks if process exists without killing it
        process.kill(existingPid, 0);
        logger.fatal({ existingPid }, 'Another NanoClaw instance is already running');
        process.exit(1);
      } catch {
        // Process doesn't exist — stale PID file, safe to overwrite
        logger.info({ stalePid: existingPid }, 'Removing stale PID file');
      }
    }
  }

  fs.writeFileSync(PID_FILE, String(process.pid));

  const cleanup = () => {
    try { fs.unlinkSync(PID_FILE); } catch {}
  };
  process.on('exit', cleanup);
  process.on('SIGINT', () => { cleanup(); process.exit(0); });
  process.on('SIGTERM', () => { cleanup(); process.exit(0); });
}

function rotateLogs(): void {
  const logDir = path.join(process.cwd(), 'logs');
  const logFile = path.join(logDir, 'nanoclaw.log');
  const errorLogFile = path.join(logDir, 'nanoclaw.error.log');
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');

  // Copy-then-truncate: launchd holds the FD open, so we can't rename.
  // Copy the old content to a rotated file, then truncate the original.
  for (const file of [logFile, errorLogFile]) {
    if (fs.existsSync(file) && fs.statSync(file).size > 0) {
      const ext = path.extname(file);
      const base = path.basename(file, ext);
      fs.copyFileSync(file, path.join(logDir, `${base}-${timestamp}${ext}`));
      fs.truncateSync(file, 0);
    }
  }

  // Keep only the 10 most recent rotated logs
  const rotated = fs.readdirSync(logDir)
    .filter(f => /^nanoclaw\.(log|error\.log)-/.test(f))
    .sort()
    .reverse();
  for (const file of rotated.slice(10)) {
    fs.unlinkSync(path.join(logDir, file));
  }
}

async function main(): Promise<void> {
  acquirePidLock();
  rotateLogs();
  ensureContainerSystemRunning();
  initDatabase();
  logger.info('Database initialized');
  loadState();

  initChannels();

  if (channels.length === 0) {
    logger.error('No channels configured. Set TELEGRAM_BOT_TOKEN in .env (or configure another channel)');
    console.error('\n╔════════════════════════════════════════════════════════════════╗');
    console.error('║  FATAL: No channels configured                                 ║');
    console.error('║                                                                ║');
    console.error('║  Set TELEGRAM_BOT_TOKEN in .env or configure another channel   ║');
    console.error('╚════════════════════════════════════════════════════════════════╝\n');
    process.exit(1);
  }

  // Start dashboard server
  if (DASHBOARD_ENABLED) {
    startDashboardServer();
  }

  // Start scheduler and message processing
  startSchedulerLoop({
    sendMessage,
    registeredGroups: () => registeredGroups,
    buildContextPrefix,
    queue: groupQueue
  });
  startIpcWatcher();
  startMessageLoop().catch((err) => {
    logger.fatal({ err }, 'Message loop crashed unexpectedly');
    process.exit(1);
  });
}

main().catch(err => {
  logger.error({ err }, 'Failed to start NanoClaw');
  process.exit(1);
});
