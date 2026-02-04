import TelegramBot from 'node-telegram-bot-api';
import pino from 'pino';
import path from 'path';
import { storeGenericMessage, storeChatMetadata } from './db.js';
import { RegisteredGroup } from './types.js';
import { GROUPS_DIR } from './config.js';
import fs from 'fs';
import https from 'https';

const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  transport: { target: 'pino-pretty', options: { colorize: true } }
});

let bot: TelegramBot | null = null;

export interface TelegramCallbacks {
  onMessage: (chatId: string, message: {
    id: string;
    sender: string;
    senderName: string;
    content: string;
    timestamp: string;
  }) => void;
  getRegisteredGroups: () => Record<string, RegisteredGroup>;
}

/**
 * Convert Telegram chat ID to a JID-like identifier for consistency.
 * Format: {chatId}@telegram
 */
export function telegramChatIdToJid(chatId: number): string {
  return `${chatId}@telegram`;
}

/**
 * Extract Telegram chat ID from JID.
 */
export function jidToTelegramChatId(jid: string): number | null {
  if (!jid.endsWith('@telegram')) return null;
  const id = parseInt(jid.replace('@telegram', ''), 10);
  return isNaN(id) ? null : id;
}

/**
 * Send a message to a Telegram chat.
 */
export async function sendTelegramMessage(jid: string, text: string): Promise<void> {
  if (!bot) {
    logger.error('Telegram bot not initialized');
    return;
  }

  const chatId = jidToTelegramChatId(jid);
  if (chatId === null) {
    logger.error({ jid }, 'Invalid Telegram JID');
    return;
  }

  try {
    await bot.sendMessage(chatId, text);
    logger.info({ chatId, length: text.length }, 'Telegram message sent');
  } catch (err) {
    logger.error({ chatId, err }, 'Failed to send Telegram message');
  }
}

/**
 * Set typing indicator for a Telegram chat.
 */
export async function setTelegramTyping(jid: string, isTyping: boolean): Promise<void> {
  if (!bot || !isTyping) return;

  const chatId = jidToTelegramChatId(jid);
  if (chatId === null) return;

  try {
    await bot.sendChatAction(chatId, 'typing');
  } catch (err) {
    logger.debug({ chatId, err }, 'Failed to send typing action');
  }
}

/**
 * Initialize and connect the Telegram bot.
 */
export function connectTelegram(callbacks: TelegramCallbacks): TelegramBot | null {
  const token = process.env.TELEGRAM_BOT_TOKEN;

  if (!token) {
    logger.info('No TELEGRAM_BOT_TOKEN found, Telegram channel disabled');
    return null;
  }

  bot = new TelegramBot(token, { polling: true });

  bot.on('message', async (msg) => {
    const chatId = msg.chat.id;
    const jid = telegramChatIdToJid(chatId);
    const timestamp = new Date(msg.date * 1000).toISOString();
    const sender = msg.from?.id?.toString() || '';
    const senderName = msg.from?.first_name || msg.from?.username || sender;
    const isFromMe = false; // Bot messages are handled separately

    // Get chat name
    const chatName = msg.chat.title || msg.chat.username || msg.chat.first_name || jid;

    // Always store chat metadata for discovery
    storeChatMetadata(jid, timestamp, chatName);

    // Handle different message types
    let content = msg.text || '';
    let hasMedia = false;

    // Handle photos
    if (msg.photo && msg.photo.length > 0) {
      try {
        // Get the highest resolution photo
        const photo = msg.photo[msg.photo.length - 1];
        const fileId = photo.file_id;

        // Ensure images directory exists (use host path for main service)
        const imagesDir = path.join(GROUPS_DIR, 'main', 'images');
        try {
          if (!fs.existsSync(imagesDir)) {
            fs.mkdirSync(imagesDir, { recursive: true });
          }
        } catch (mkdirErr) {
          logger.warn({ imagesDir, mkdirErr }, 'Could not create images directory, it may already exist');
        }

        // Get file info
        const file = await bot!.getFile(fileId);

        if (file.file_path) {
          // Use simpler approach: get file link and download with fetch
          const fileUrl = `https://api.telegram.org/file/bot${process.env.TELEGRAM_BOT_TOKEN}/${file.file_path}`;
          const localPath = path.join(imagesDir, `${fileId}.jpg`);

          const response = await fetch(fileUrl);
          const buffer = await response.arrayBuffer();
          fs.writeFileSync(localPath, Buffer.from(buffer));

          // Path for containers to access the image
          const containerPath = `/workspace/group/images/${fileId}.jpg`;
          content = `[Image: ${containerPath}]${msg.caption ? '\n' + msg.caption : ''}`;
          hasMedia = true;
          logger.info({ chatId, hostPath: localPath, containerPath }, 'Photo downloaded');
        } else {
          content = `[Image - no file path]${msg.caption ? '\n' + msg.caption : ''}`;
        }
      } catch (err) {
        const errorMsg = err instanceof Error ? err.message : String(err);
        const errorStack = err instanceof Error ? err.stack : undefined;
        logger.error({ chatId, err, errorMsg, errorStack }, 'Failed to download photo');
        content = `[Image - download failed: ${errorMsg}]${msg.caption ? '\n' + msg.caption : ''}`;
      }
    }

    // Skip if no content at all
    if (!content) return;

    // Only store full message content for registered groups
    const registeredGroups = callbacks.getRegisteredGroups();
    if (registeredGroups[jid]) {
      storeGenericMessage(
        msg.message_id.toString(),
        jid,
        sender,
        senderName,
        content,
        timestamp,
        isFromMe,
        'telegram'
      );

      // Notify the main app about the message
      callbacks.onMessage(jid, {
        id: msg.message_id.toString(),
        sender,
        senderName,
        content,
        timestamp
      });
    }
  });

  bot.on('polling_error', (error) => {
    logger.error({ error: error.message }, 'Telegram polling error');
  });

  logger.info('Connected to Telegram');
  return bot;
}

/**
 * Check if Telegram is connected.
 */
export function isTelegramConnected(): boolean {
  return bot !== null;
}
